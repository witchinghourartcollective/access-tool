import pytest
from sqlalchemy.orm import Session

from core.models.chat import TelegramChatUser
from core.services.chat.user import TelegramChatUserService
from tests.factories import TelegramChatFactory, TelegramChatUserFactory, UserFactory


@pytest.mark.asyncio
async def test_yield_all_for_chat_batching(db_session: Session) -> None:
    # Setup
    chat = TelegramChatFactory.with_session(db_session).create()
    service = TelegramChatUserService(db_session)

    # Create 25 users
    users = []
    for i in range(25):
        user = UserFactory.with_session(db_session).create(telegram_id=1000 + i)
        chat_user = TelegramChatUserFactory.with_session(db_session).create(
            chat=chat, user=user, is_admin=False, is_managed=True
        )
        users.append(chat_user)

    # Test
    batches: list[list[TelegramChatUser]] = []
    for batch in service.yield_all_for_chat(chat.id, batch_size=10):
        batches.append(batch)

    # Verify
    assert len(batches) == 3
    assert len(batches[0]) == 10
    assert len(batches[1]) == 10
    assert len(batches[2]) == 5

    all_yielded_users = [u for batch in batches for u in batch]
    assert len(all_yielded_users) == 25

    # Verify order
    user_ids = [u.user_id for u in all_yielded_users]
    assert user_ids == sorted(user_ids)
