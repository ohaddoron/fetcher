from pydantic import BaseModel


class Settings(BaseModel):
    db_name: str = 'omics-database'


def get_settings() -> Settings:
    return Settings()
