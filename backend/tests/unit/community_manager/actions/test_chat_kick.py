import pytest
from unittest.mock import AsyncMock

from community_manager.actions.chat import CommunityManagerUserChatAction
from core.models.chat import TelegramChatUser, TelegramChat, User


@pytest.mark.asyncio
async def test_kick_chat_member_admin_protection(db_session):
    action = CommunityManagerUserChatAction(db_session)
    chat = TelegramChat(id=1, title="Test Chat", is_full_control=True)
    user = User(id=1, telegram_id=123)
    chat_user = TelegramChatUser(
        user_id=1, chat_id=1, is_admin=True, is_managed=True, chat=chat, user=user
    )

    # Mock bot_api_service to ensure it is NOT called
    action.bot_api_service = AsyncMock()

    await action.kick_chat_member(chat_user)

    action.bot_api_service.kick_chat_member.assert_not_called()


@pytest.mark.asyncio
async def test_kick_chat_member_normal_user(db_session):
    action = CommunityManagerUserChatAction(db_session)
    chat = TelegramChat(id=1, title="Test Chat", is_full_control=True)
    user = User(id=1, telegram_id=123)
    chat_user = TelegramChatUser(
        user_id=1, chat_id=1, is_admin=False, is_managed=True, chat=chat, user=user
    )

    # Mock bot_api_service to ensure it is called
    action.bot_api_service = AsyncMock()
    # Mock delete
    action.telegram_chat_user_service.delete = (
        AsyncMock()
    )  # actually sync method, but mocked on instance?
    # actually telegram_chat_user_service is initialized in __init__
    # we can mock the whole service or specific method
    action.telegram_chat_user_service = AsyncMock()

    await action.kick_chat_member(chat_user)

    action.bot_api_service.kick_chat_member.assert_awaited_once()
