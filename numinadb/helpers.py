#
# Copyright 2016-2017 Universidad Complutense de Madrid
#
# This file is part of Numina
#
# Numina is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# Numina is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with Numina.  If not, see <http://www.gnu.org/licenses/>.
#

"""User command line interface of Numina."""

from __future__ import print_function

import os
import json

import numina.user.helpers
import numina.types.qc
from numina.types.product import DataProductTag
from numina.util.jsonencoder import ExtEncoder

from .model import DataProduct, ReductionResult, ReductionResultValue
from .dbkeys import DB_PRODUCT_KEYS


def store_to(result, where):
    import numina.store
    print('calling my store_to')
    saveres = {}
    for key, prod in result.stored().items():
        val = getattr(result, key)

        where.destination = prod.dest
        saveres[key] = numina.store.dump(prod.type, val, where)

    return saveres


class ProcessingTask(numina.user.helpers.ProcessingTask):
    def __init__(self, session, obsres=None, runinfo=None):
        self.session = session
        super(ProcessingTask, self).__init__(obsres, runinfo)



class Backend(object):
    def __init__(self, session):
        self.logfile = 'processing.log'
        self.session = session

    def store(self, task, where):
        print('calling store to ', where)
        # save to disk the RecipeResult part and return the file to save it
        # saveres = self.result.store_to(where)

        saveres = store_to(task.result, where)

        self.post_result_store(task, saveres)

        with open(where.result, 'w+') as fd:
            json.dump(saveres, fd, indent=2, cls=ExtEncoder)

        out = {}
        out['observation'] = task.observation
        out['result'] = where.result
        out['runinfo'] = task.runinfo

        relpathdir = os.path.relpath(task.runinfo['results_dir'], task.runinfo['base_dir'])

        full_logfile = os.path.join(relpathdir, self.logfile)
        full_task = os.path.join(relpathdir, where.task)
        full_result = os.path.join(relpathdir, where.result)

        with open(where.task, 'w+') as fd:
            json.dump(out, fd, indent=2, cls=ExtEncoder)

        result = {'logs': full_logfile, 'task': full_task, 'result': full_result}

        return result

    def post_result_store(self, task, saveres):
        session = self.session
        result = task.result
        result_db = ReductionResult()

        # print(self.runinfo)
        # print(self.observation)
        result_db.instrument_id = task.observation['instrument']

        result_db.pipeline = task.runinfo['pipeline']
        result_db.obsmode = task.observation['mode']
        result_db.recipe = task.runinfo['recipe_full_name']

        # datatype = Column(String(45))
        result_db.task_id = task.runinfo['taskid']
        result_db.ob_id = task.observation['observing_result']
        # dateobs = Column(DateTime)
        if hasattr(result, 'qc'):
            result_db.qc = result.qc

        session.add(result_db)
        for key, prod in result.stored().items():
            if prod.dest != 'qc':

                val = ReductionResultValue()
                fullpath = os.path.join(task.runinfo['results_dir'], saveres[prod.dest])
                relpath = os.path.relpath(fullpath, task.runinfo['base_dir'])
                val.name = prod.dest
                val.datatype = prod.type.name()
                val.contents = relpath
                result_db.values.append(val)

                if isinstance(prod.type, DataProductTag):
                    product = DataProduct(datatype=prod.type.name(),
                                          task_id=task.runinfo['taskid'],
                                          instrument_id=task.observation['instrument'],
                                          contents=relpath
                                          )
                    product.result_value = val
                    internal_value = getattr(result, key)
                    meta_info = prod.type.extract_db_info(internal_value, DB_PRODUCT_KEYS)
                    product.dateobs = meta_info['observation_date']
                    product.uuid = meta_info['uuid']
                    product.qc = meta_info['quality_control']
                    master_tags = meta_info['tags']
                    for k, v in master_tags.items():
                        product[k] = v

                    session.add(product)

        session.commit()


def build_mdir(taskid, obsid):
    mdir = "task_{0:03d}_{1}".format(taskid, obsid)
    return mdir


class WorkEnvironment(numina.user.helpers.WorkEnvironment):
    def __init__(self, basedir, datadir, taskid, task_obid):
        mdir = build_mdir(taskid, task_obid)
        workdir = os.path.join(basedir, mdir, 'work')
        resultsdir = os.path.join(basedir, mdir, 'results')

        if datadir is None:
            datadir = os.path.join(basedir, 'data')

        super(WorkEnvironment, self).__init__("1", basedir, workdir, resultsdir, datadir)
