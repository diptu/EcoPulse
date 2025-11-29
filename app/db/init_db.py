# app/db/init_db.py
"""
Async-safe initialization for database models.
"""

# Ensure all models are imported so they are registered with Base
import app.models  # noqa: F401


def import_all_models():
    """
    Dummy function to ensure all models are imported before table creation.
    Import each model here if needed to guarantee registration with Base.
    """
    import app.models.upload  # noqa: F401

    # add other models as needed
