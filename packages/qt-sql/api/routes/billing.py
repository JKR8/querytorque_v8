"""Billing and subscription routes.

POST /api/v1/billing/checkout   — Create Stripe checkout session
POST /api/v1/billing/portal     — Create Stripe billing portal session
GET  /api/v1/billing/usage      — Get current period usage
POST /api/v1/billing/webhook    — Stripe webhook handler
"""

import logging

from fastapi import APIRouter, Depends, HTTPException, Request
from pydantic import BaseModel
from sqlalchemy.ext.asyncio import AsyncSession

from qt_shared.auth.middleware import CurrentUser
from qt_shared.config import get_settings
from qt_shared.database.connection import get_async_session

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/api/v1/billing", tags=["Billing"])


class CheckoutRequest(BaseModel):
    tier: str
    success_url: str
    cancel_url: str


class CheckoutResponse(BaseModel):
    checkout_url: str
    session_id: str


class PortalResponse(BaseModel):
    portal_url: str


class UsageResponse(BaseModel):
    tier: str
    analyses_today: int
    analyses_limit: int
    api_calls_this_month: int
    api_calls_limit: int
    llm_tokens_this_month: int


@router.post("/checkout", response_model=CheckoutResponse)
async def create_checkout(
    request: CheckoutRequest,
    user: CurrentUser,
    session: AsyncSession = Depends(get_async_session),
):
    """Create a Stripe checkout session to upgrade tier."""
    from qt_shared.billing.service import BillingService

    billing = BillingService(session)
    checkout = await billing.create_checkout_session(
        org_id=user.org_id,
        tier=request.tier,
        success_url=request.success_url,
        cancel_url=request.cancel_url,
    )

    return CheckoutResponse(
        checkout_url=checkout["url"],
        session_id=checkout["session_id"],
    )


@router.post("/portal", response_model=PortalResponse)
async def create_portal(
    user: CurrentUser,
    session: AsyncSession = Depends(get_async_session),
):
    """Create a Stripe billing portal session for self-service management."""
    from qt_shared.billing.service import BillingService

    billing = BillingService(session)
    portal = await billing.create_billing_portal_session(org_id=user.org_id)

    return PortalResponse(portal_url=portal["url"])


@router.get("/usage", response_model=UsageResponse)
async def get_usage(
    user: CurrentUser,
    session: AsyncSession = Depends(get_async_session),
):
    """Get current billing period usage for the user's organization."""
    from qt_shared.billing.service import BillingService
    from qt_shared.config import get_tier_features

    billing = BillingService(session)
    usage = await billing.get_current_usage(org_id=user.org_id)
    tier = getattr(user, "tier", "free")
    features = get_tier_features(tier)

    return UsageResponse(
        tier=tier,
        analyses_today=usage.get("analyses_today", 0),
        analyses_limit=features.get("analyses_per_day", 1),
        api_calls_this_month=usage.get("api_calls_this_month", 0),
        api_calls_limit=features.get("api_calls_per_month", 0),
        llm_tokens_this_month=usage.get("llm_tokens_this_month", 0),
    )


@router.post("/webhook")
async def stripe_webhook(
    request: Request,
    session: AsyncSession = Depends(get_async_session),
):
    """Handle Stripe webhook events.

    Verifies webhook signature and processes subscription events.
    """
    import stripe

    settings = get_settings()
    if not settings.stripe_webhook_secret:
        raise HTTPException(status_code=501, detail="Stripe webhooks not configured")

    body = await request.body()
    sig = request.headers.get("stripe-signature", "")

    try:
        event = stripe.Webhook.construct_event(
            body, sig, settings.stripe_webhook_secret,
        )
    except (ValueError, stripe.error.SignatureVerificationError):
        raise HTTPException(status_code=400, detail="Invalid webhook signature")

    from qt_shared.billing.service import BillingService
    billing = BillingService(session)

    event_type = event["type"]
    data = event["data"]["object"]

    if event_type == "customer.subscription.created":
        await billing.handle_subscription_created(data)
    elif event_type == "customer.subscription.updated":
        await billing.handle_subscription_updated(data)
    elif event_type == "customer.subscription.deleted":
        await billing.handle_subscription_deleted(data)
    else:
        logger.info("Unhandled Stripe event: %s", event_type)

    return {"received": True}
