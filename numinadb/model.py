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

import datetime

from sqlalchemy.ext.declarative import declarative_base
# from sqlalchemy import UniqueConstraint, ForeignKeyConstraint, PrimaryKeyConstraint, CheckConstraint, desc
from sqlalchemy import Integer, String, DateTime, Float, Boolean, TIMESTAMP, Unicode, UnicodeText
from sqlalchemy import Table, Column, ForeignKey, UniqueConstraint
# from sqlalchemy import PickleType, Enum

from sqlalchemy.orm import relationship, backref, synonym
from sqlalchemy.orm.collections import attribute_mapped_collection
# from sqlalchemy.orm import validates

from .jsonsqlite import MagicJSON

import numina.core.dataframe


Base = declarative_base()


class MyOb(Base):
    __tablename__ = 'obs'

    id = Column(String, primary_key=True)
    instrument = Column(String, nullable=False)
    mode = Column(String, nullable=False)
    start_time = Column(DateTime, default=datetime.datetime.utcnow)
    completion_time = Column(DateTime)
    frames = relationship("Frame", back_populates='ob')

    facts = relationship('Fact', secondary='data_obs_fact')
    children = []


class Fact(Base):
    """A fact about an OB."""

    __tablename__ = 'fact'

    id = Column(Integer, primary_key=True)
    key = Column(String(64))
    type = Column(String(64))
    value = Column(String(1))

    __mapper_args__ = {
        'polymorphic_identity': 'fact',
        'polymorphic_on': type
    }


class FactString(Fact):
    """A string fact about an OB."""

    __tablename__ = 'fact_string'

    id = Column(Integer, ForeignKey('fact.id'), primary_key=True)
    value = Column(String(64))

    __mapper_args__ = {
        'polymorphic_identity': 'fact_string',
    }


class FactInt(Fact):
    """A int fact about an OB."""

    __tablename__ = 'fact_int'

    id = Column(Integer, ForeignKey('fact.id'), primary_key=True)
    value = Column(Integer)

    __mapper_args__ = {
        'polymorphic_identity': 'fact_int',
    }


class FactFloat(Fact):
    """A float fact about an OB."""

    __tablename__ = 'fact_float'

    id = Column(Integer, ForeignKey('fact.id'), primary_key=True)
    value = Column(Float)

    __mapper_args__ = {
        'polymorphic_identity': 'fact_float',
    }


class Frame(Base):
    __tablename__ = 'frames'
    id = Column(Integer, primary_key=True)
    name = Column(String(10), unique=True, nullable=False)
    ob_id = Column(String,  ForeignKey("obs.id"), nullable=False)
    ob = relationship("MyOb", back_populates='frames')
    #
    filename = synonym("name")

    def open(self):
        from astropy.io import fits
        return fits.open(self.name, mode='readonly')

    def to_numina_frame(self):
        return numina.core.dataframe.DataFrame(filename=self.filename)


class Task(Base):
    __tablename__ = 'tasks'
    id = Column(Integer, primary_key=True)
    ob_id = Column(Integer,  ForeignKey("obs.id"), nullable=False)
    ob = relationship("MyOb")
    start_time = Column(DateTime, default=datetime.datetime.utcnow)
    completion_time = Column(DateTime)


class DataProduct(Base):
    __tablename__ = 'products'

    id = Column(Integer, primary_key=True)
    instrument_id = Column(String(10))
    datatype = Column(String(45))
    task_id = Column(Integer, ForeignKey('tasks.id'))
    contents = Column(String(45))
    priority = Column(Integer, default=0)
    facts = relationship('Fact', secondary='data_products_fact')


data_products_fact = Table(
    'data_products_fact', Base.metadata,
    Column('product_id', Integer, ForeignKey('products.id'), primary_key=True),
    Column('fact_id', Integer, ForeignKey('fact.id'), primary_key=True)
)


data_obs_fact = Table(
    'data_obs_fact', Base.metadata,
    Column('obs_id', Integer, ForeignKey('obs.id'), primary_key=True),
    Column('fact_id', Integer, ForeignKey('fact.id'), primary_key=True)
)


recipe_parameters_fact = Table(
    'recipe_parameters_fact', Base.metadata,
    Column('param_value_id', Integer, ForeignKey('recipe_parameter_values.id'), primary_key=True),
    Column('fact_id', Integer, ForeignKey('fact.id'), primary_key=True)
)


class RecipeParameters(Base):
    __tablename__ = 'recipe_parameters'
    __table_args__ = (UniqueConstraint('instrument', 'pipeline', 'mode', 'name'), )

    id = Column(Integer, primary_key=True)
    instrument = Column(String, nullable=False)
    pipeline = Column(String, default='default', nullable=False)
    mode = Column(String(100), nullable=False)
    name = Column(String(100), nullable=False)
    values = relationship("RecipeParameterValues", back_populates='parameter')


class RecipeParameterValues(Base):
    __tablename__ = 'recipe_parameter_values'

    id = Column(Integer, primary_key=True)
    param_id = Column(String,  ForeignKey("recipe_parameters.id"), nullable=False)

    content = Column(MagicJSON, nullable=False)
    facts = relationship('Fact', secondary='recipe_parameters_fact')
    parameter = relationship("RecipeParameters")
