from app.adapter.repository.abstractmodel import (
    AbstractModel,
    ModelFactory,
)


class NEWAVE(AbstractModel):
    pass


ModelFactory().register("newave", NEWAVE)
