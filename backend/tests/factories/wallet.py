import factory
from core.models.wallet import UserWallet, JettonWallet
from tests.factories.base import BaseSQLAlchemyModelFactory
from tests.factories.jetton import JettonFactory
from tests.factories.user import UserFactory


class UserWalletFactory(BaseSQLAlchemyModelFactory):
    class Meta:
        model = UserWallet
        sqlalchemy_session_persistence = "flush"

    address = factory.Faker("pystr", min_chars=65, max_chars=65, prefix="0:")
    user = factory.SubFactory(UserFactory)
    balance = factory.Faker("random_int", min=0, max=1000000000)
    hide_wallet = False


class JettonWalletFactory(BaseSQLAlchemyModelFactory):
    class Meta:
        model = JettonWallet
        sqlalchemy_session_persistence = "flush"
        exclude = ("jetton",)

    address = factory.Faker("pystr", min_chars=65, max_chars=65, prefix="0:")
    jetton_master_address = factory.SelfAttribute("jetton.address")
    jetton = factory.SubFactory(JettonFactory)
    owner_address = factory.Faker("pystr", min_chars=65, max_chars=65, prefix="0:")
    balance = factory.Faker("random_int", min=0, max=1000000)
