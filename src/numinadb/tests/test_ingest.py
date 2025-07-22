import pkgutil

import astropy.io.fits as fits
import numina.core.pipelineload
import numina.datamodel
from numina.drps.drpbase import DrpGeneric
from numina.instrument.assembly import assembly_instrument
import numpy as np
import pytest

from sqlalchemy import select
import yaml

from ..ingest import ingest_ob_file, metadata_fits
from ..model import ObservingBlock, Instrument


def create_drp1():
    drp_data1 = pkgutil.get_data('numina.drps.tests', 'drptest1.yaml')
    drp = numina.core.pipelineload.drp_load_data('numina', drp_data1)
    return drp


def create_drps():
    drp = create_drp1()
    drps = DrpGeneric({'TEST1': drp})
    return drps


def test_drp1():
    drp = create_drp1()
    assert isinstance(drp.datamodel, numina.datamodel.DataModel)
    assert len(drp.datamodel.db_info_keys) == 11


def create_test1_image(drp, mode, n=1):
    comp_store = drp.configurations
    ins_name = "TEST1"
    date_str = "2020-01-01T12:00:00"
    ins1 = assembly_instrument(comp_store, ins_name, date_str)

    for idx in range(n):
        header = fits.Header()
        header['INSTRUME'] = 'TEST1'
        header['OBSMODE'] = mode
        header['DATE-OBS'] = date_str
        header['EXPTIME'] = 0.0
        header['OBJECT'] = mode
        header['INSCONF'] =  str(ins1.uuid)
        data = np.array([[0, 0]])
        hdu = fits.PrimaryHDU(data=data, header=header)
        hdulist = fits.HDUList([hdu])
        yield hdulist


@pytest.fixture
def create_ob1(tmp_path):
    ob_file = tmp_path / "ob.yaml"
    obs_blocks = []
    ob_data = dict(id=2, instrument='TEST1', mode="BIAS", images=[], children=[], parent=None, facts=None)
    obs_blocks.append(ob_data)
    ob_data = dict(id=3, instrument='TEST1', mode="DARK", images=[], children=[], parent=None, facts=None)
    obs_blocks.append(ob_data)
    with open(ob_file, "w") as fd:
        yaml.safe_dump_all(obs_blocks, fd)
    return tmp_path


@pytest.fixture
def create_ob2(tmp_path):
    ob_file = tmp_path / "ob.yaml"
    ob_data = dict(id=2, instrument='TEST1', mode="BIAS", frames=[], children=[], parent=None, facts=None)
    datadir = tmp_path / "data"
    datadir.mkdir()
    n = 4
    drp1 = create_drp1()
    for idx, hdulist in enumerate(create_test1_image(drp1, 'BIAS', n)):
        fits_name = f'r00{idx:02d}.fits'
        ob_data['frames'].append(fits_name)
        hdulist.writeto(datadir / fits_name)

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


def test_ingest_ob2(db_session, create_ob2):
    ob_file = create_ob2 / "ob.yaml"
    drps = create_drps()
    ingest_ob_file(db_session, ob_file, drps=drps)
    qr = select(ObservingBlock)
    res = db_session.scalars(qr)
    for r in res:
        assert r.instrument_id == 'TEST1'

    qr2 = select(Instrument)
    res2 = db_session.scalars(qr2)
    for r2 in res2:
        assert r2.name == 'TEST1'


def test_ingest_fits():
    drps = create_drps()
    drp = drps.query_by_name('TEST1')
    hdul = next(create_test1_image(drp, mode="BIAS"))
    res = metadata_fits(hdul, drps=create_drps())
    assert res['exptime'] == 0.0
    assert res['darktime'] == 0.0
    assert res['object'] == 'BIAS'
    assert res['insconf'] == "225fcaf2-7f6f-49cc-972a-70fd0aee8e96"
