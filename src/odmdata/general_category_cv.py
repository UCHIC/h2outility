from sqlalchemy import Column, String

from odmdata.base import Base


class GeneralCategoryCV(Base):
	__tablename__ = 'GeneralCategoryCV'

	term   	   = Column('Term', String, primary_key=True)
	definition = Column('Definition', String)

	def __repr__(self):
		return "<GeneralCategoryCV('%s', '%s')>" % (self.term, self.definition)
