def test_skeleton_imports():
    import fleet  # noqa: F401
    from fleet.api import create_app  # noqa: F401
    from fleet.service import RolloutService  # noqa: F401
