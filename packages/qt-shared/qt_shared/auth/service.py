"""Authentication service for QueryTorque API."""

import uuid
from datetime import datetime
from typing import Optional

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from ..database.models import User, Organization, Subscription
from .middleware import generate_api_key
from .context import UserContext


class AuthService:
    """Service for authentication operations."""

    def __init__(self, session: AsyncSession):
        self.session = session

    async def get_or_create_user(
        self,
        auth0_id: str,
        email: str,
        name: Optional[str] = None,
    ) -> User:
        """Get or create a user from Auth0 authentication.

        Creates organization and subscription if user is new.
        """
        # Check if user exists
        result = await self.session.execute(
            select(User).where(User.auth0_id == auth0_id)
        )
        user = result.scalar_one_or_none()

        if user:
            # Update last login
            user.last_login = datetime.utcnow()
            await self.session.commit()
            return user

        # Create new organization for the user
        org = Organization(
            name=name or email.split("@")[0],
            tier="free",
        )
        self.session.add(org)
        await self.session.flush()

        # Create user
        user = User(
            email=email,
            auth0_id=auth0_id,
            org_id=org.id,
            role="owner",
            last_login=datetime.utcnow(),
        )
        self.session.add(user)

        # Create free subscription
        subscription = Subscription(
            org_id=org.id,
            tier="free",
            status="active",
        )
        self.session.add(subscription)

        await self.session.commit()
        return user

    async def get_user_by_id(self, user_id: uuid.UUID) -> Optional[User]:
        """Get user by ID."""
        result = await self.session.execute(
            select(User).where(User.id == user_id)
        )
        return result.scalar_one_or_none()

    async def get_user_by_email(self, email: str) -> Optional[User]:
        """Get user by email."""
        result = await self.session.execute(
            select(User).where(User.email == email)
        )
        return result.scalar_one_or_none()

    async def get_user_by_auth0_id(self, auth0_id: str) -> Optional[User]:
        """Get user by Auth0 ID."""
        result = await self.session.execute(
            select(User).where(User.auth0_id == auth0_id)
        )
        return result.scalar_one_or_none()

    async def generate_user_api_key(self, user_id: uuid.UUID) -> Optional[str]:
        """Generate a new API key for a user.

        Returns the full API key (only returned once).
        """
        user = await self.get_user_by_id(user_id)
        if not user:
            return None

        full_key, prefix, key_hash = generate_api_key()

        user.api_key_hash = key_hash
        user.api_key_prefix = prefix
        await self.session.commit()

        return full_key

    async def revoke_user_api_key(self, user_id: uuid.UUID) -> bool:
        """Revoke a user's API key."""
        user = await self.get_user_by_id(user_id)
        if not user:
            return False

        user.api_key_hash = None
        user.api_key_prefix = None
        await self.session.commit()
        return True

    async def get_organization(self, org_id: uuid.UUID) -> Optional[Organization]:
        """Get organization by ID."""
        result = await self.session.execute(
            select(Organization).where(Organization.id == org_id)
        )
        return result.scalar_one_or_none()

    async def get_user_organization(self, user_id: uuid.UUID) -> Optional[Organization]:
        """Get the organization for a user."""
        user = await self.get_user_by_id(user_id)
        if not user:
            return None

        return await self.get_organization(user.org_id)

    async def add_user_to_organization(
        self,
        email: str,
        org_id: uuid.UUID,
        role: str = "member",
    ) -> Optional[User]:
        """Add a new user to an existing organization."""
        existing = await self.get_user_by_email(email)
        if existing:
            return None

        user = User(
            email=email,
            org_id=org_id,
            role=role,
        )
        self.session.add(user)
        await self.session.commit()
        return user

    async def update_user_role(
        self,
        user_id: uuid.UUID,
        role: str,
    ) -> Optional[User]:
        """Update a user's role."""
        user = await self.get_user_by_id(user_id)
        if not user:
            return None

        user.role = role
        await self.session.commit()
        return user

    async def deactivate_user(self, user_id: uuid.UUID) -> bool:
        """Deactivate a user."""
        user = await self.get_user_by_id(user_id)
        if not user:
            return False

        user.is_active = False
        user.api_key_hash = None
        user.api_key_prefix = None
        await self.session.commit()
        return True

    async def get_organization_users(self, org_id: uuid.UUID) -> list[User]:
        """Get all users in an organization."""
        result = await self.session.execute(
            select(User).where(User.org_id == org_id).where(User.is_active == True)
        )
        return list(result.scalars().all())

    async def get_user_context(self, user: User) -> UserContext:
        """Build UserContext from User model."""
        org = await self.get_organization(user.org_id)
        return UserContext(
            user_id=str(user.id),
            org_id=str(user.org_id),
            email=user.email,
            role=user.role,
            tier=org.tier if org else "free",
            auth_method="database",
        )
