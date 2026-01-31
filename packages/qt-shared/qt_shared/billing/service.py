"""Billing service for QueryTorque API."""

import stripe
from typing import Optional
from datetime import datetime
from uuid import UUID

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from ..config import get_settings
from ..database.models import Organization, Subscription, User, APIUsage, AnalysisJob
from .tiers import TIER_FEATURES


class BillingService:
    """Service for billing and subscription operations."""

    def __init__(self, session: AsyncSession):
        self.session = session
        settings = get_settings()
        if settings.stripe_api_key:
            stripe.api_key = settings.stripe_api_key

    async def get_subscription(self, org_id: UUID) -> Optional[Subscription]:
        """Get subscription for an organization."""
        result = await self.session.execute(
            select(Subscription).where(Subscription.org_id == org_id)
        )
        return result.scalar_one_or_none()

    async def get_organization(self, org_id: UUID) -> Optional[Organization]:
        """Get organization by ID."""
        result = await self.session.execute(
            select(Organization).where(Organization.id == org_id)
        )
        return result.scalar_one_or_none()

    async def create_checkout_session(
        self,
        org_id: UUID,
        price_id: str,
        success_url: str,
        cancel_url: str,
    ) -> Optional[str]:
        """Create a Stripe checkout session for subscription.

        Returns the checkout session URL.
        """
        settings = get_settings()
        if not settings.stripe_api_key:
            return None

        org = await self.get_organization(org_id)
        if not org:
            return None

        # Create or get Stripe customer
        if not org.stripe_customer_id:
            customer = stripe.Customer.create(
                name=org.name,
                metadata={"org_id": str(org_id)},
            )
            org.stripe_customer_id = customer.id
            await self.session.commit()

        # Create checkout session
        checkout_session = stripe.checkout.Session.create(
            customer=org.stripe_customer_id,
            payment_method_types=["card"],
            line_items=[
                {
                    "price": price_id,
                    "quantity": 1,
                }
            ],
            mode="subscription",
            success_url=success_url,
            cancel_url=cancel_url,
            metadata={
                "org_id": str(org_id),
            },
        )

        return checkout_session.url

    async def create_billing_portal_session(
        self,
        org_id: UUID,
        return_url: str,
    ) -> Optional[str]:
        """Create a Stripe billing portal session.

        Returns the portal session URL.
        """
        settings = get_settings()
        if not settings.stripe_api_key:
            return None

        org = await self.get_organization(org_id)
        if not org or not org.stripe_customer_id:
            return None

        portal_session = stripe.billing_portal.Session.create(
            customer=org.stripe_customer_id,
            return_url=return_url,
        )

        return portal_session.url

    async def handle_subscription_created(
        self,
        stripe_subscription_id: str,
        stripe_customer_id: str,
        price_id: str,
        status: str,
        current_period_start: int,
        current_period_end: int,
    ) -> Optional[Subscription]:
        """Handle subscription.created webhook event."""
        result = await self.session.execute(
            select(Organization).where(Organization.stripe_customer_id == stripe_customer_id)
        )
        org = result.scalar_one_or_none()
        if not org:
            return None

        tier = self._price_id_to_tier(price_id)

        sub = await self.get_subscription(org.id)
        if sub:
            sub.stripe_subscription_id = stripe_subscription_id
            sub.stripe_price_id = price_id
            sub.tier = tier
            sub.status = status
            sub.current_period_start = datetime.fromtimestamp(current_period_start)
            sub.current_period_end = datetime.fromtimestamp(current_period_end)
        else:
            sub = Subscription(
                org_id=org.id,
                stripe_subscription_id=stripe_subscription_id,
                stripe_price_id=price_id,
                tier=tier,
                status=status,
                current_period_start=datetime.fromtimestamp(current_period_start),
                current_period_end=datetime.fromtimestamp(current_period_end),
            )
            self.session.add(sub)

        org.tier = tier

        await self.session.commit()
        return sub

    async def handle_subscription_updated(
        self,
        stripe_subscription_id: str,
        price_id: str,
        status: str,
        current_period_start: int,
        current_period_end: int,
        cancel_at_period_end: bool,
    ) -> Optional[Subscription]:
        """Handle subscription.updated webhook event."""
        result = await self.session.execute(
            select(Subscription).where(Subscription.stripe_subscription_id == stripe_subscription_id)
        )
        sub = result.scalar_one_or_none()
        if not sub:
            return None

        tier = self._price_id_to_tier(price_id)

        sub.stripe_price_id = price_id
        sub.tier = tier
        sub.status = status
        sub.current_period_start = datetime.fromtimestamp(current_period_start)
        sub.current_period_end = datetime.fromtimestamp(current_period_end)
        sub.cancel_at_period_end = cancel_at_period_end

        org = await self.get_organization(sub.org_id)
        if org:
            org.tier = tier

        await self.session.commit()
        return sub

    async def handle_subscription_deleted(
        self,
        stripe_subscription_id: str,
    ) -> Optional[Subscription]:
        """Handle subscription.deleted webhook event."""
        result = await self.session.execute(
            select(Subscription).where(Subscription.stripe_subscription_id == stripe_subscription_id)
        )
        sub = result.scalar_one_or_none()
        if not sub:
            return None

        sub.tier = "free"
        sub.status = "canceled"

        org = await self.get_organization(sub.org_id)
        if org:
            org.tier = "free"

        await self.session.commit()
        return sub

    def _price_id_to_tier(self, price_id: str) -> str:
        """Convert Stripe price ID to tier name."""
        settings = get_settings()

        price_map = {
            settings.stripe_price_free: "free",
            settings.stripe_price_workspace_pro: "workspace_pro",
            settings.stripe_price_capacity: "capacity",
            settings.stripe_price_enterprise: "enterprise",
        }

        return price_map.get(price_id, "free")

    def get_tier_price_id(self, tier: str) -> Optional[str]:
        """Get Stripe price ID for a tier."""
        settings = get_settings()

        tier_map = {
            "free": settings.stripe_price_free,
            "workspace_pro": settings.stripe_price_workspace_pro,
            "capacity": settings.stripe_price_capacity,
            "enterprise": settings.stripe_price_enterprise,
        }

        return tier_map.get(tier)

    def get_tier_features(self, tier: str) -> dict:
        """Get features for a tier."""
        return TIER_FEATURES.get(tier, TIER_FEATURES["free"])

    async def get_current_usage(self, org_id: UUID) -> dict:
        """Get current usage statistics for an organization."""
        from sqlalchemy import func

        now = datetime.utcnow()
        period_start = now.replace(day=1, hour=0, minute=0, second=0, microsecond=0)

        # Get API call count
        result = await self.session.execute(
            select(func.count(APIUsage.id))
            .where(APIUsage.org_id == org_id)
            .where(APIUsage.created_at >= period_start)
        )
        api_calls = result.scalar() or 0

        # Get analysis count (today)
        today_start = now.replace(hour=0, minute=0, second=0, microsecond=0)
        result = await self.session.execute(
            select(func.count(AnalysisJob.id))
            .where(AnalysisJob.user_id.in_(
                select(User.id).where(User.org_id == org_id)
            ))
            .where(AnalysisJob.created_at >= today_start)
        )
        analyses_today = result.scalar() or 0

        # Get subscription info
        sub = await self.get_subscription(org_id)
        tier = sub.tier if sub else "free"
        features = self.get_tier_features(tier)

        return {
            "api_calls_this_month": api_calls,
            "api_calls_limit": features.get("api_calls_per_month", 0),
            "analyses_today": analyses_today,
            "analyses_limit": features.get("analyses_per_day", 1),
            "period_start": period_start.isoformat(),
            "tier": tier,
        }
