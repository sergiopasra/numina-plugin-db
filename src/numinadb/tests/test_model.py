
from sqlalchemy import MetaData


def test_model(db_session):
    """Test expected tables are created"""
    expected_tables = ['data_obs_fact', 'obs', 'instruments', 'fact', 'dp_task',
                       'frames', 'obs_alias', 'parameter_facts',
                       'recipe_parameter_values', 'recipe_parameters',
                       'product_facts', 'products', 'reduction_result_values',
                       'reduction_results']
    metadata = MetaData()
    metadata.reflect(bind=db_session.get_bind())
    tables = list(metadata.tables.keys())
    assert sorted(tables) == sorted(expected_tables)
