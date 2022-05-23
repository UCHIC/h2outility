from sqlalchemy import Column, String

from odmdata.base import Base


class SpeciationCV(Base):
	__tablename__ = 'SpeciationCV'

	term   	   = Column('Term', String, primary_key=True)
	definition = Column('Definition', String)

	def __repr__(self):
		return "<SpeciationCV('%s', '%s')>" % (self.term, self.definition)
