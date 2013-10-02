from sqlalchemy.testing import fixtures, eq_
from sqlalchemy.testing.schema import Table, Column
from sqlalchemy.orm import Bundle, Session
from sqlalchemy.testing import AssertsCompiledSQL
from sqlalchemy import Integer, select
from sqlalchemy.orm import mapper

class BundleTest(fixtures.MappedTest, AssertsCompiledSQL):
    __dialect__ = 'default'

    run_inserts = 'once'
    run_setup_mappers = 'once'
    run_deletes = None

    @classmethod
    def define_tables(cls, metadata):
        Table('data', metadata,
                Column('id', Integer, primary_key=True,
                            test_needs_autoincrement=True),
                Column('d1', Integer),
                Column('d2', Integer),
                Column('d3', Integer)
            )

    @classmethod
    def setup_classes(cls):
        class Data(cls.Basic):
            pass

    @classmethod
    def setup_mappers(cls):
        mapper(cls.classes.Data, cls.tables.data)

    @classmethod
    def insert_data(cls):
        sess = Session()
        sess.add_all([
            cls.classes.Data(d1='d%dd1' % i, d2='d%dd2' % i, d3='d%dd3' % i)
            for i in range(10)
        ])
        sess.commit()

    def test_c_attr(self):
        Data = self.classes.Data

        b1 = Bundle('b1', Data.d1, Data.d2)

        self.assert_compile(
            select([b1.c.d1, b1.c.d2]),
            "SELECT data.d1, data.d2 FROM data"
        )

    def test_result(self):
        Data = self.classes.Data
        sess = Session()

        b1 = Bundle('b1', Data.d1, Data.d2)

        eq_(
            sess.query(b1).filter(b1.c.d1.between('d3d1', 'd5d1')).all(),
            [(('d3d1', 'd3d2'),), (('d4d1', 'd4d2'),), (('d5d1', 'd5d2'),)]
        )

    def test_subclass(self):
        Data = self.classes.Data
        sess = Session()

        class MyBundle(Bundle):
            def create_row_processor(self, query, procs, labels):
                def proc(row, result):
                    return dict(
                                zip(labels, (proc(row, result) for proc in procs))
                            )
                return proc

        b1 = MyBundle('b1', Data.d1, Data.d2)

        eq_(
            sess.query(b1).filter(b1.c.d1.between('d3d1', 'd5d1')).all(),
            [({'d2': 'd3d2', 'd1': 'd3d1'},),
                ({'d2': 'd4d2', 'd1': 'd4d1'},),
                ({'d2': 'd5d2', 'd1': 'd5d1'},)]
        )

    def test_bundle_nesting(self):
        Data = self.classes.Data
        sess = Session()

        b1 = Bundle('b1', Data.d1, Bundle('b2', Data.d2, Data.d3))

        eq_(
            sess.query(b1).
                filter(b1.c.d1.between('d3d1', 'd7d1')).
                filter(b1.c.b2.c.d2.between('d4d2', 'd6d2')).
                all(),
            [(('d4d1', ('d4d2', 'd4d3')),), (('d5d1', ('d5d2', 'd5d3')),),
                (('d6d1', ('d6d2', 'd6d3')),)]
        )







