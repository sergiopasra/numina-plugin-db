import astropy.io.fits as fits
import numpy as np
import pytest
from sqlalchemy import select
import yaml

from ..ingest import ingest_ob_file
from ..model import ObservingBlock, Instrument


@pytest.fixture
def create_ob1(tmp_path):
    ob_file = tmp_path / "ob.yaml"
    obs_blocks = []
    ob_data = dict(id=2, instrument='TEST1', mode="mode1", images=[], children=[], parent=None, facts=None)
    obs_blocks.append(ob_data)
    ob_data = dict(id=3, instrument='TEST1', mode="mode2", images=[], children=[], parent=None, facts=None)
    obs_blocks.append(ob_data)
    with open(ob_file, "w") as fd:
        yaml.safe_dump_all(obs_blocks, fd)
    return tmp_path


@pytest.fixture
def create_ob2(tmp_path):
    ob_file = tmp_path / "ob.yaml"
    ob_data = dict(id=2, instrument='TEST1', mode="mode1", frames=[], children=[], parent=None, facts=None)
    datadir = tmp_path / "data"
    datadir.mkdir()
    n = 4
    for idx in range(n):
        fits_name = f'r00{idx:02d}.fits'
        ob_data['frames'].append(fits_name)
        header = fits.Header()
        header['INSTRUME'] = 'TEST1'
        fits.writeto(datadir / fits_name, np.array([[0, 0]]), header=header)

    with open(ob_file, "w") as fd:
        yaml.safe_dump_all([ob_data], fd)
    return tmp_path


def test_ingest_ob1(db_session, create_ob1):
    ob_file = create_ob1 / "ob.yaml"
    ingest_ob_file(db_session, ob_file)
    qr = select(ObservingBlock)
    res = db_session.scalars(qr)
    for r in res:
        assert r.instrument_id == 'TEST1'

    qr2 = select(Instrument)
    res2 = db_session.scalars(qr2)
    for r2 in res2:
        assert r2.name == 'TEST1'


@pytest.mark.xfail(reason="No pipeline for instrument TEST1")
def test_ingest_ob2(db_session, create_ob2):
    ob_file = create_ob2 / "ob.yaml"
    ingest_ob_file(db_session, ob_file)
    qr = select(ObservingBlock)
    res = db_session.scalars(qr)
    for r in res:
        assert r.instrument_id == 'TEST1'

    qr2 = select(Instrument)
    res2 = db_session.scalars(qr2)
    for r2 in res2:
        assert r2.name == 'TEST1'
    assert False
