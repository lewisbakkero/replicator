# Shared pytest fixtures for the mcps test suite.
#
# Subsequent tasks (data models, adapters, CLI, integration tests) will populate
# this file with fixtures such as fake SourceAdapters, seeded Catalogs, and
# moto-backed S3 clients. For now it is intentionally empty so pytest discovers
# the tests/ tree and the unit/, integration/, smoke/ subdirectories without
# pulling in dependencies that the scaffold task does not yet require.
