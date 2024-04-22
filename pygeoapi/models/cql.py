# ****************************** -*-
# flake8: noqa
# generated by datamodel-codegen:
#   filename:  cql-schema.json
#   timestamp: 2021-03-13T21:05:20+00:00
# =================================================================
#
# Authors: Francesco Bartoli <xbartolone@gmail.com>
#
# Copyright (c) 2024 Francesco Bartoli
#
# Permission is hereby granted, free of charge, to any person
# obtaining a copy of this software and associated documentation
# files (the "Software"), to deal in the Software without
# restriction, including without limitation the rights to use,
# copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the
# Software is furnished to do so, subject to the following
# conditions:
#
# The above copyright notice and this permission notice shall be
# included in all copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
# EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES
# OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
# NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT
# HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY,
# WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
# FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
# OTHER DEALINGS IN THE SOFTWARE.
#
# =================================================================

from datetime import date, datetime
from typing import Any, List, Literal, Optional, Union

from pydantic import BaseModel, Field


class CQLModel(BaseModel):
    __root__: 'Union[\n        ComparisonPredicate,\n        SpatialPredicate,\n        TemporalPredicate,\n        AndExpression\n    ]'


class AndExpression(BaseModel):
    and_: 'List[ComparisonPredicate]' = Field(..., alias='and')


class NotExpression(BaseModel):
    not_: 'List[Any]' = Field(..., alias='not')


class OrExpression(BaseModel):
    or_: 'List[Any]' = Field(..., alias='or')


class PropertyRef(BaseModel):
    property: 'Optional[str]' = None


class ScalarLiteral(BaseModel):
    __root__: 'Union[str, float, bool]'


class Bbox(BaseModel):
    __root__: 'List[float]'


class LinestringCoordinate(BaseModel):
    __root__: 'List[Any]'


class Linestring(BaseModel):
    type: Literal['LineString']
    coordinates: 'List[LinestringCoordinate]' = Field(...)
    bbox: 'Optional[List[float]]' = Field(None)


class MultilineStringCoordinate(BaseModel):
    __root__: 'List[Any]'


class Multilinestring(BaseModel):
    type: Literal['MultiLineString']
    coordinates: 'List[List[MultilineStringCoordinate]]'
    bbox: 'Optional[List[float]]' = Field(None)


class Multipoint(BaseModel):
    type: Literal['MultiPoint']
    coordinates: 'List[List[float]]'
    bbox: 'Optional[List[float]]' = Field(None)


class MultipolygonCoordinateItem(BaseModel):
    __root__: 'List[Any]'


class Multipolygon(BaseModel):
    type: Literal['MultiPolygon']
    coordinates: 'List[List[List[MultipolygonCoordinateItem]]]'
    bbox: 'Optional[List[float]]' = Field(None)


class Point(BaseModel):
    type: Literal['Point']
    coordinates: 'List[float]' = Field(...)
    bbox: 'Optional[List[float]]' = Field(None)


class PolygonCoordinatesItem(BaseModel):
    __root__: 'List[Any]'


class Polygon(BaseModel):
    type: Literal['Polygon']
    coordinates: 'List[List[PolygonCoordinatesItem]]'
    bbox: 'Optional[List[float]]' = Field(None)


class TimeString(BaseModel):
    __root__: 'Union[date, datetime]'


class EnvelopeLiteral(BaseModel):
    bbox: 'Bbox'


class GeometryLiteral(BaseModel):
    __root__: 'Union[\n        Point,\n        Linestring,\n        Polygon,\n        Multipoint,\n        Multilinestring,\n        Multipolygon\n    ]'


class TypedTimeString(BaseModel):
    datetime: 'TimeString'


class PeriodString(BaseModel):
    __root__: 'List[Union[TimeString, str]]' = Field(...)


class SpatialLiteral(BaseModel):
    __root__: 'Union[GeometryLiteral, EnvelopeLiteral]'


class TemporalLiteral(BaseModel):
    __root__: 'Union[TimeString, PeriodString]'


class TypedPeriodString(BaseModel):
    datetime: 'PeriodString'


class TypedTemporalLiteral(BaseModel):
    __root__: 'Union[TypedTimeString, TypedPeriodString]'


class ArrayPredicate(BaseModel):
    __root__: 'Union[\n        AequalsExpression,\n        AcontainsExpression,\n        AcontainedByExpression,\n        AoverlapsExpression,\n    ]'


class ComparisonPredicate(BaseModel):
    __root__: 'Union[\n        BinaryComparisonPredicate,\n        IsLikePredicate,\n        IsBetweenPredicate,\n        IsInListPredicate,\n        IsNullPredicate,\n    ]'


class SpatialPredicate(BaseModel):
    __root__: 'Union[\n        IntersectsExpression,\n        EqualsExpression,\n        DisjointExpression,\n        TouchesExpression,\n        WithinExpression,\n        OverlapsExpression,\n        CrossesExpression,\n        ContainsExpression,\n    ]'


class TemporalPredicate(BaseModel):
    __root__: 'Union[\n        BeforeExpression,\n        AfterExpression,\n        MeetsExpression,\n        MetbyExpression,\n        ToverlapsExpression,\n        OverlappedbyExpression,\n        BeginsExpression,\n        BegunbyExpression,\n        DuringExpression,\n        TcontainsExpression,\n        EndsExpression,\n        EndedbyExpression,\n        TequalsExpression,\n        AnyinteractsExpression,\n    ]'


class AcontainedByExpression(BaseModel):
    acontainedBy: 'ArrayExpression'


class AcontainsExpression(BaseModel):
    acontains: 'ArrayExpression'


class AequalsExpression(BaseModel):
    aequals: 'ArrayExpression'


class AfterExpression(BaseModel):
    after: 'TemporalOperands'


class AnyinteractsExpression(BaseModel):
    anyinteracts: 'TemporalOperands'


class AoverlapsExpression(BaseModel):
    aoverlaps: 'ArrayExpression'


class BeforeExpression(BaseModel):
    before: 'TemporalOperands'


class BeginsExpression(BaseModel):
    begins: 'TemporalOperands'


class BegunbyExpression(BaseModel):
    begunby: 'TemporalOperands'


class BinaryComparisonPredicate(BaseModel):
    __root__: 'Union[\n        EqExpression, LtExpression, GtExpression, LteExpression, GteExpression\n    ]'


class ContainsExpression(BaseModel):
    contains: 'SpatialOperands'


class CrossesExpression(BaseModel):
    crosses: 'SpatialOperands'


class DisjointExpression(BaseModel):
    disjoint: 'SpatialOperands'


class DuringExpression(BaseModel):
    during: 'TemporalOperands'


class EndedbyExpression(BaseModel):
    endedby: 'TemporalOperands'


class EndsExpression(BaseModel):
    ends: 'TemporalOperands'


class EqualsExpression(BaseModel):
    equals: 'SpatialOperands'


class IntersectsExpression(BaseModel):
    intersects: 'SpatialOperands'


class Between(BaseModel):
    value: 'ValueExpression'
    lower: 'ScalarExpression' = Field(None)
    upper: 'ScalarExpression' = Field(None)


class IsBetweenPredicate(BaseModel):
    between: 'Between'


class In(BaseModel):
    value: 'ValueExpression'
    list: 'List[ValueExpression]'
    nocase: 'Optional[bool]' = True


class IsInListPredicate(BaseModel):
    in_: 'In' = Field(..., alias='in')


class IsLikePredicate(BaseModel):
    like: 'ScalarOperands'
    wildcard: 'Optional[str]' = '%'
    singleChar: 'Optional[str]' = '.'
    escapeChar: 'Optional[str]' = '\\'
    nocase: 'Optional[bool]' = True


class IsNullPredicate(BaseModel):
    isNull: 'ScalarExpression'


class MeetsExpression(BaseModel):
    meets: 'TemporalOperands'


class MetbyExpression(BaseModel):
    metby: 'TemporalOperands'


class OverlappedbyExpression(BaseModel):
    overlappedby: 'TemporalOperands'


class OverlapsExpression(BaseModel):
    overlaps: 'SpatialOperands'


class TcontainsExpression(BaseModel):
    tcontains: 'TemporalOperands'


class TequalsExpression(BaseModel):
    tequals: 'TemporalOperands'


class TouchesExpression(BaseModel):
    touches: 'SpatialOperands'


class ToverlapsExpression(BaseModel):
    toverlaps: 'TemporalOperands'


class WithinExpression(BaseModel):
    within: 'SpatialOperands'


class ArrayExpression(BaseModel):
    __root__: 'List[Union[PropertyRef, FunctionRef, ArrayLiteral]]' = Field(
        ...  # , max_items=2, min_items=2
    )


class EqExpression(BaseModel):
    eq: 'ScalarOperands'


class GtExpression(BaseModel):
    gt: 'ScalarOperands'


class GteExpression(BaseModel):
    gte: 'ScalarOperands'


class LtExpression(BaseModel):
    lt: 'ScalarOperands'


class LteExpression(BaseModel):
    lte: 'ScalarOperands'


class ScalarExpression(BaseModel):
    __root__: 'Union[ScalarLiteral,\n        PropertyRef,\n        FunctionRef,\n        ArithmeticExpression]'


class ScalarOperands(BaseModel):
    __root__: 'List[ScalarExpression]' = Field(...)


class SpatialOperands(BaseModel):
    __root__: 'List[GeomExpression]' = Field(...)


class TemporalOperands(BaseModel):
    __root__: 'List[TemporalExpression]' = Field(...)
    # , max_items=2, min_items=2)


class ValueExpression(BaseModel):
    __root__: 'Union[ScalarExpression, SpatialLiteral, TypedTemporalLiteral]'


class ArithmeticExpression(BaseModel):
    __root__: 'Union[AddExpression, SubExpression, MulExpression, DivExpression]'


class ArrayLiteral(BaseModel):
    __root__: 'List[\n        Union[\n            ScalarLiteral,\n            SpatialLiteral,\n            TypedTemporalLiteral,\n            PropertyRef,\n            FunctionRef,\n            ArithmeticExpression,\n            ArrayLiteral,\n        ]\n    ]'


class FunctionRef(BaseModel):
    function: 'Function'


class GeomExpression(BaseModel):
    __root__: 'Union[SpatialLiteral, PropertyRef, FunctionRef]'


class TemporalExpression(BaseModel):
    __root__: 'Union[TemporalLiteral, PropertyRef, FunctionRef]'


# here
class AddExpression(BaseModel):
    add_: 'ArithmeticOperands' = Field(..., alias='+')


# here
class DivExpression(BaseModel):
    div_: 'ArithmeticOperands' = Field(None, alias='/')


class Function(BaseModel):
    name: 'str'
    arguments: 'Optional[\n        List[\n            Union[\n                ScalarLiteral,\n                SpatialLiteral,\n                TypedTemporalLiteral,\n                PropertyRef,\n                FunctionRef,\n                ArithmeticExpression,\n                ArrayLiteral,\n            ]\n        ]\n    ]' = None


# here
class MulExpression(BaseModel):
    mul_: 'ArithmeticOperands' = Field(..., alias='*')


# here
class SubExpression(BaseModel):
    sub_: 'ArithmeticOperands' = Field(..., alias='-')


class ArithmeticOperands(BaseModel):
    __root__: 'List[\n        Union[ArithmeticExpression, PropertyRef, FunctionRef, float]\n    ]' = Field(...)


CQLModel.update_forward_refs()
AndExpression.update_forward_refs()
ArrayPredicate.update_forward_refs()
ComparisonPredicate.update_forward_refs()
SpatialPredicate.update_forward_refs()
TemporalPredicate.update_forward_refs()
AcontainedByExpression.update_forward_refs()
AcontainsExpression.update_forward_refs()
AequalsExpression.update_forward_refs()
AfterExpression.update_forward_refs()
AnyinteractsExpression.update_forward_refs()
AoverlapsExpression.update_forward_refs()
BeforeExpression.update_forward_refs()
BeginsExpression.update_forward_refs()
BegunbyExpression.update_forward_refs()
BinaryComparisonPredicate.update_forward_refs()
ContainsExpression.update_forward_refs()
CrossesExpression.update_forward_refs()
DisjointExpression.update_forward_refs()
DuringExpression.update_forward_refs()
EndedbyExpression.update_forward_refs()
EndsExpression.update_forward_refs()
EqualsExpression.update_forward_refs()
IntersectsExpression.update_forward_refs()
Between.update_forward_refs()
In.update_forward_refs()
IsBetweenPredicate.update_forward_refs()
IsLikePredicate.update_forward_refs()
IsNullPredicate.update_forward_refs()
ValueExpression.update_forward_refs()
MeetsExpression.update_forward_refs()
MetbyExpression.update_forward_refs()
OverlappedbyExpression.update_forward_refs()
OverlapsExpression.update_forward_refs()
TcontainsExpression.update_forward_refs()
TequalsExpression.update_forward_refs()
TouchesExpression.update_forward_refs()
ToverlapsExpression.update_forward_refs()
WithinExpression.update_forward_refs()
ArrayExpression.update_forward_refs()
EqExpression.update_forward_refs()
GtExpression.update_forward_refs()
GteExpression.update_forward_refs()
LtExpression.update_forward_refs()
LteExpression.update_forward_refs()
ScalarExpression.update_forward_refs()
ScalarOperands.update_forward_refs()
SpatialOperands.update_forward_refs()
TemporalOperands.update_forward_refs()
ArithmeticExpression.update_forward_refs()
ArrayLiteral.update_forward_refs()
ScalarLiteral.update_forward_refs()
PropertyRef.update_forward_refs()
FunctionRef.update_forward_refs()
AddExpression.update_forward_refs()
DivExpression.update_forward_refs()
MulExpression.update_forward_refs()
SubExpression.update_forward_refs()


def get_next_node(obj):
    logical_op = None
    if obj.__repr_name__() == 'AndExpression':
        next_node = obj.and_
        logical_op = 'and'
    elif obj.__repr_name__() == 'OrExpression':
        next_node = obj.or_
        logical_op = 'or'
    elif obj.__repr_name__() == 'NotExpression':
        next_node = obj.not_
        logical_op = 'not'
    elif obj.__repr_name__() == 'ComparisonPredicate':
        next_node = obj.__root__
    elif obj.__repr_name__() == 'SpatialPredicate':
        next_node = obj.__root__
    elif obj.__repr_name__() == 'TemporalPredicate':
        next_node = obj.__root__
    elif obj.__repr_name__() == 'IsBetweenPredicate':
        next_node = obj.between
    elif obj.__repr_name__() == 'Between':
        next_node = obj.value
    elif obj.__repr_name__() == 'ValueExpression':
        next_node = obj.__root__ or obj.lower or obj.upper
    elif obj.__repr_name__() == 'ScalarExpression':
        next_node = obj.__root__
    elif obj.__repr_name__() == 'ScalarLiteral':
        next_node = obj.__root__
    elif obj.__repr_name__() == 'PropertyRef':
        next_node = obj.property
    elif obj.__repr_name__() == 'BinaryComparisonPredicate':
        next_node = obj.__root__
    elif obj.__repr_name__() == 'EqExpression':
        next_node = obj.eq
        logical_op = 'eq'
    else:
        raise ValueError("Object not valid")

    return (logical_op, next_node)