import numina.datamodel

DB_PRODUCT_KEYS = [
    'instrument',
    'observation_date',
    'uuid',
    'quality_control'
]

# Candidate for removal
DB_FRAME_KEYS = numina.datamodel.DataModel.db_info_keys.extend(['vph', 'insmode'])
