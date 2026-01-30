import factory
from core.models.blockchain import NFTCollection, NftItem
from tests.factories.base import BaseSQLAlchemyModelFactory


class NFTCollectionFactory(BaseSQLAlchemyModelFactory):
    class Meta:
        model = NFTCollection
        sqlalchemy_session_persistence = "flush"

    address = factory.Faker("pystr", min_chars=65, max_chars=65, prefix="0:")
    name = factory.Faker("word")
    description = factory.Faker("text")
    is_enabled = True


class NftItemFactory(BaseSQLAlchemyModelFactory):
    class Meta:
        model = NftItem
        sqlalchemy_session_persistence = "flush"
        exclude = ("collection",)

    address = factory.Faker("pystr", min_chars=65, max_chars=65, prefix="0:")
    owner_address = factory.Faker("pystr", min_chars=65, max_chars=65, prefix="0:")
    collection_address = factory.SelfAttribute("collection.address")
    collection = factory.SubFactory(NFTCollectionFactory)
