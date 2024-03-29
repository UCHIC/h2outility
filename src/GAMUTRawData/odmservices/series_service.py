import logging
import threading

import pandas
from sqlalchemy import distinct, func

from GAMUTRawData.odmdata import DataValue, Method, ODMVersion, OffsetType, Qualifier, QualityControlLevel, Sample, \
    Series, SessionFactory, Site, Unit, Variable

logger = logging.getLogger('main')


class TimeoutException(Exception):
    def __init__(self, *args):
        super(TimeoutException, self).__init__(*args)


class SeriesService():
    # Accepts a string for creating a SessionFactory, default uses odmdata/connection.cfg
    def __init__(self, connection_string="", debug=False):
        self._session_factory = SessionFactory(connection_string, debug)
        self._edit_session = self._session_factory.get_session()
        self._debug = debug

    def reset_session(self):
        self._edit_session = self._session_factory.get_session()  # Reset the session in order to prevent memory leaks

    def get_db_version(self):
        return self._edit_session.query(ODMVersion).first().version_number

    #####################
    #
    # Get functions
    #
    #####################

    # Site methods
    def get_all_sites(self):
        """

        :return: List[Sites]
        """
        return self._edit_session.query(Site).order_by(Site.code).all()

    def get_used_sites(self):
        """
        Return a list of all sites that are being referenced in the Series Catalog Table
        :return: List[Sites]
        """
        try:
            site_ids = [x[0] for x in self._edit_session.query(distinct(Series.site_id)).all()]
        except:
            site_ids = None

        if not site_ids:
            return None

        Sites = []
        for site_id in site_ids:
            Sites.append(self._edit_session.query(Site).filter_by(id=site_id).first())

        return Sites

    def get_site_by_id(self, site_id):
        """
        return a Site object that has an id=site_id
        :param site_id: integer- the identification number of the site
        :return: Sites
        """
        try:
            return self._edit_session.query(Site).filter_by(id=site_id).first()
        except:
            return None

    def get_site_by_code(self, site_code):
        """
        return a Site object that has an id=site_code
        :param site_code: str- the identification code of the site
        :return: Sites
        """
        try:
            return self._edit_session.query(Site).filter_by(code=site_code).first()
        except:
            return None

    # Variables methods
    def get_used_variables(self):
        """
        #get list of used variable ids
        :return: List[Variables]
        """

        try:
            var_ids = [x[0] for x in self._edit_session.query(distinct(Series.variable_id)).all()]
        except:
            var_ids = None

        if not var_ids:
            return None

        Variables = []

        # create list of variables from the list of ids
        for var_id in var_ids:
            Variables.append(self._edit_session.query(Variable).filter_by(id=var_id).first())

        return Variables

    def get_all_variables(self):
        """

        :return: List[Variables]
        """
        return self._edit_session.query(Variable).all()

    def get_variable_by_id(self, variable_id):
        """

        :param variable_id: int
        :return: Variables
        """
        try:
            return self._edit_session.query(Variable).filter_by(id=variable_id).first()
        except:
            return None

    def get_variable_by_code(self, variable_code):
        """

        :param variable_code:  str
        :return: Variables
        """
        try:
            return self._edit_session.query(Variable).filter_by(code=variable_code).first()
        except:
            return None

    def get_variables_by_site_code(self, site_code):  # covers NoDV, VarUnits, TimeUnits
        """
        Finds all of variables at a site
        :param site_code: str
        :return: List[Variables]
        """
        try:
            var_ids = [x[0] for x in self._edit_session.query(distinct(Series.variable_id)).filter_by(
                    site_code=site_code).all()]
        except:
            var_ids = None

        variables = []
        for var_id in var_ids:
            variables.append(self._edit_session.query(Variable).filter_by(id=var_id).first())

        return variables

    # Unit methods
    def get_all_units(self):
        """

        :return: List[Units]
        """
        return self._edit_session.query(Unit).all()

    def get_unit_by_name(self, unit_name):
        """

        :param unit_name: str
        :return: Units
        """
        try:
            return self._edit_session.query(Unit).filter_by(name=unit_name).first()
        except:
            return None

    def get_unit_by_id(self, unit_id):
        """

        :param unit_id: int
        :return: Units
        """
        try:
            return self._edit_session.query(Unit).filter_by(id=unit_id).first()
        except:
            return None

    def get_all_qualifiers(self):
        """

        :return: List[Qualifiers]
        """
        result = self._edit_session.query(Qualifier).order_by(Qualifier.code).all()
        return result

    def get_qualifier_by_code(self, code):
        """

        :return: Qualifiers
        """
        result = self._edit_session.query(Qualifier).filter(Qualifier.code == code).first()
        return result

    def get_qualifiers_by_series_id(self, series_id):
        """

        :param series_id:
        :return:
        """
        subquery = self._edit_session.query(DataValue.qualifier_id)\
            .outerjoin(Series.data_values)\
            .filter(Series.id == series_id, DataValue.qualifier_id != None)\
            .distinct()\
            .subquery()

        return self._edit_session.query(Qualifier).join(subquery).distinct().all()

    def get_qualifiers_by_series_details(self, site_id, qc_id, source_id, method_id, var_ids, year=None):
        """

        :param series_id:
        :return:
        """
        subquery = self._edit_session.query(DataValue.qualifier_id).outerjoin(
                Series.data_values).filter(Series.site_id == site_id,
                                           DataValue.site_id == site_id,
                                           DataValue.variable_id == var_ids,
                                           DataValue.variable_id == Variable.id,
                                           DataValue.quality_control_level_id == qc_id,
                                           DataValue.source_id == source_id,
                                           DataValue.method_id == method_id,
                                           DataValue.qualifier_id != None).distinct().subquery()
        return self._edit_session.query(Qualifier).join(subquery).distinct().all()

    # QCL methods
    def get_all_qcls(self):
        return self._edit_session.query(QualityControlLevel).all()

    def get_qcl_by_id(self, qcl_id):
        try:
            return self._edit_session.query(QualityControlLevel).filter_by(id=qcl_id).first()
        except:
            return None

    def get_qcl_by_code(self, qcl_code):
        try:
            return self._edit_session.query(QualityControlLevel).filter_by(code=qcl_code).first()
        except:
            return None

    # Method methods
    def get_all_methods(self):
        return self._edit_session.query(Method).all()

    def get_method_by_id(self, method_id):
        try:
            result = self._edit_session.query(Method).filter_by(id=method_id).first()
        except:
            result = None
        return result

    def get_method_by_description(self, method_code):
        try:
            result = self._edit_session.query(Method).filter_by(description=method_code).first()
        except:
            result = None
            logger.error("method not found")
        return result

    def get_offset_types_by_series_id(self, series_id):
        """

        :param series_id:
        :return:
        """
        subquery = self._edit_session.query(DataValue.offset_type_id).outerjoin(
                Series.data_values).filter(Series.id == series_id,
                                           DataValue.offset_type_id != None).distinct().subquery()
        return self._edit_session.query(OffsetType).join(subquery).distinct().all()

    def get_samples_by_series_id(self, series_id):
        """

        :param series_id:
        :return:
        """
        subquery = self._edit_session.query(DataValue.sample_id).outerjoin(
                Series.data_values).filter(Series.id == series_id, DataValue.sample_id != None).distinct().subquery()
        return self._edit_session.query(Sample).join(subquery).distinct().all()

    # Series Catalog methods
    def get_all_series(self):
        """
        Returns all series as a modelObject
        :return: List[Series]
        """

        # logger.debug("%s" % self._edit_session.query(Series).order_by(Series.id).all())
        return self._edit_session.query(Series).order_by(Series.id).all()

    def get_series_by_site(self, site_id):
        """

        :param site_id: int
        :return: List[Series]
        """
        try:
            selectedSeries = self._edit_session.query(Series).filter_by(site_id=site_id).order_by(Series.id).all()
            return selectedSeries
        except:
            return None

    def get_series_by_id(self, series_id):
        """

        :param series_id: int
        :return: Series
        """
        try:
            return self._edit_session.query(Series).filter_by(id=series_id).first()
        except Exception as e:
            print e
            return None

    def get_series_by_site_code_year(self, site_code, year):
        try:
            # vals = self._edit_session.query(Series).filter_by(site_code=site_code, quality_control_level_id=0). \
            #     filter(Series.end_date_time.between(year + '-01-01', str(int(year) + 1) + '-01-01')).all()
            year_start = '{}-01-01 00:00:00'.format(year)
            year_end = '{}-12-31 23:59:59'.format(year)
            vals = self._edit_session.query(Series).filter(Series.site_code == site_code,
                                                           Series.quality_control_level_id == 0,
                                                           Series.end_date_time.between(year_start, year_end)).all()
            return vals
        except Exception as ex:
            return None

    def get_series_by_site_and_qc_level(self, site_code, qc):
        try:
            values = self._edit_session.query(Series).filter(Series.site_code == site_code,
                                                             Series.quality_control_level_id == qc).all()
            return values
        except Exception as ex:
            return None

    def get_var_by_site(self, my_site_id):
        try:
            var = self._edit_session.query(Series.variable_code, Series.variable_id).filter(
                    Series.site_id == my_site_id).order_by(Series.variable_id).distinct().all()
            return var
        except Exception as e:
            print (e)
            return []

    def get_all_values_by_site_id_date(self, my_site_id, first_date_time, end_date_time):
        try:
            q = self._edit_session.query(DataValue, Variable.code).filter(DataValue.site_id == my_site_id,
                                                                          DataValue.local_date_time >= first_date_time,
                                                                          DataValue.local_date_time <= end_date_time,
                                                                          DataValue.variable_id == Variable.id,
                                                                          DataValue.quality_control_level_id == 0)

            query = q.statement.compile(dialect=self._session_factory.engine.dialect)
            array = pandas.read_sql_query(sql=query, con=self._session_factory.engine, params=query.params)
            return array
        except MemoryError as e:
            print 'Memory Error encountered during query!!\nError: {}\n'.format(type(e), e)
        except Exception as e:
            print 'Unexpected error encountered during query\nType: {}\nError: {}\n\n'.format(type(e), e)
            print e

    def get_all_values_by_site_id(self, my_site_id):
        try:
            q = self._edit_session.query(DataValue, Variable.code).filter(DataValue.site_id == my_site_id,
                                                                          # DataValue.local_date_time >= first_date_time,
                                                                          # DataValue.local_date_time <= end_date_time,
                                                                          DataValue.variable_id == Variable.id,
                                                                          DataValue.quality_control_level_id == 0)

            query = q.statement.compile(dialect=self._session_factory.engine.dialect)
            array = pandas.read_sql_query(sql=query, con=self._session_factory.engine, params=query.params)
            return array
        except MemoryError as e:
            print 'Memory Error encountered during query!!\nError: {}\n'.format(type(e), e)
        except Exception as e:
            print 'Unexpected error encountered during query\nType: {}\nError: {}\n\n'.format(type(e), e)
            print e

    def get_values_by_filters(self, site_id, qc_id, source_id, method_ids, var_ids, year=None, starting_date=None,
                              chunk_size=250000, timeout=100000, is_retry=False):
        try:
            if qc_id != 0 or len(var_ids) == 1 or len(method_ids) == 1:
                query_items = self._edit_session.query(DataValue.date_time_utc, DataValue.local_date_time,
                                                       DataValue.utc_offset, DataValue.data_value,
                                                       DataValue.qualifier_id, DataValue.censor_code, Variable.code,
                                                       DataValue.method_id)
            else:
                query_items = self._edit_session.query(DataValue.date_time_utc, DataValue.local_date_time,
                                                       DataValue.utc_offset, DataValue.data_value, Variable.code,
                                                       DataValue.method_id)

            if year is None and starting_date is None:
                q = query_items.filter(DataValue.site_id == site_id, DataValue.variable_id.in_(var_ids),
                                       DataValue.variable_id == Variable.id,
                                       DataValue.quality_control_level_id == qc_id, DataValue.source_id == source_id,
                                       DataValue.method_id.in_(method_ids))

            elif year is not None and starting_date is None:
                year_start = '{}-01-01 00:00:00'.format(year)
                year_end = '{}-12-31 23:59:59'.format(year)
                q = query_items.filter(DataValue.local_date_time.between(year_start, year_end),
                                       DataValue.site_id == site_id, DataValue.variable_id.in_(var_ids),
                                       DataValue.variable_id == Variable.id,
                                       DataValue.quality_control_level_id == qc_id, DataValue.source_id == source_id,
                                       DataValue.method_id.in_(method_ids))

            elif year is None and starting_date is not None:
                q = query_items.filter(DataValue.site_id == site_id, DataValue.variable_id.in_(var_ids),
                                       DataValue.variable_id == Variable.id,
                                       DataValue.quality_control_level_id == qc_id, DataValue.source_id == source_id,
                                       DataValue.method_id.in_(method_ids),
                                       DataValue.local_date_time > starting_date)

            else:
                year_start = '{}-01-01 00:00:00'.format(year)
                year_end = '{}-12-31 23:59:59'.format(year)
                q = query_items.filter(DataValue.local_date_time.between(year_start, year_end),
                                       DataValue.site_id == site_id, DataValue.variable_id.in_(var_ids),
                                       DataValue.variable_id == Variable.id,
                                       DataValue.quality_control_level_id == qc_id, DataValue.source_id == source_id,
                                       DataValue.method_id.in_(method_ids),
                                       DataValue.local_date_time > starting_date)

            query = q.statement.compile(dialect=self._session_factory.engine.dialect)

            return pandas.read_sql_query(query, self._session_factory.engine, params=query.params, coerce_float=True)

            # result = None
            # for chunk in pandas.read_sql_query(sql=query, con=self._session_factory.engine, params=query.params,
            #                                    coerce_float=True, chunksize=chunk_size):
            #     result = chunk if result is None else pandas.concat([result, chunk], copy=False)
            #
            # return result
        except MemoryError as e:
            print 'Memory Error encountered during query!!\nError: {}\n'.format(type(e), e)
        except TimeoutException as e:
            print 'Timeout: {}'.format(e)
            if is_retry:
                return None
            else:
                print 'First query timed out - retrying'
                return self.get_values_by_filters(site_id, qc_id, source_id, method_ids, var_ids, year=year,
                                                  chunk_size=chunk_size, timeout=timeout, is_retry=True)
        except Exception as e:
            print 'Unexpected error encountered during query\nType: {}\nError: {}\n\n'.format(type(e), e)
            print e

    def get_variables_by_site_id_qc(self, variable_id, my_site_id, qc):
        """

        :param variable_id: Variable ID for filtering query
        :type variable_id: int
        :param my_site_id: Site ID to filter query
        :type my_site_id: int
        :param qc: Quality Control Level ID to use
        :type qc: int
        :return: DataFrame of results
        :rtype: pandas.DataFrame
        """
        try:
            q = self._edit_session.query(DataValue, Variable.code).filter(
                    my_site_id == DataValue.site_id,
                    DataValue.variable_id == Variable.id,
                    DataValue.variable_id == variable_id,
                    DataValue.quality_control_level_id == qc)

            query = q.statement.compile(dialect=self._session_factory.engine.dialect)
            return pandas.read_sql_query(sql=query, con=self._session_factory.engine, params=query.params,
                                         coerce_float=False)
        except Exception as e:
            print e
            return []

    def get_series_by_id_quint(self, site_id, var_id, method_id, source_id, qcl_id):
        """

        :param site_id:
        :param var_id:
        :param method_id:
        :param source_id:
        :param qcl_id:
        :return: Series
        """
        try:
            return self._edit_session.query(Series).filter_by(
                    site_id=site_id, variable_id=var_id, method_id=method_id,
                    source_id=source_id, quality_control_level_id=qcl_id).first()
        except:
            return None

    def raise_timeout_exception(self):
        raise TimeoutException('SQL Query took too long')

    def get_series_from_filter(self, site_id, variable_id, qc_level_id, source_id, method_id):
        try:
            query_result = self._edit_session.query(Series).filter(Series.site_id == site_id,
                                                                   Series.variable_id == variable_id,
                                                                   Series.quality_control_level_id == qc_level_id,
                                                                   Series.source_id == source_id,
                                                                   Series.method_id == method_id)
            return query_result.first()
        except Exception as e:
            print e
            return []

    # Data Value Methods
    def get_values_by_series(self, series_id):
        '''

        :param series_id:  Series id
        :return: pandas dataframe
        '''
        series = self.get_series_by_id(series_id)
        if series:
            q = self._edit_session.query(DataValue).filter_by(
                    site_id=series.site_id,
                    variable_id=series.variable_id,
                    method_id=series.method_id,
                    source_id=series.source_id,
                    quality_control_level_id=series.quality_control_level_id)

            query = q.statement.compile(dialect=self._session_factory.engine.dialect)
            data = pandas.read_sql_query(sql=query,
                                         con=self._session_factory.engine,
                                         params=query.params)
            # return data.set_index(data['LocalDateTime'])
            return data
        else:
            return None

    # Data Value Methods
    def get_values_by_series_and_year(self, series, year=None):
        '''

        :param series_id:  Series id
        :return: pandas dataframe
        '''
        if year:
            year_start = '{}-01-01 00:00:00'.format(year)
            year_end = '{}-12-31 23:59:59'.format(year)
            q = self._edit_session.query(DataValue).filter(Series.end_date_time.between(year_start, year_end),
                                                           site_id=series.site_id,
                                                           variable_id=series.variable_id,
                                                           method_id=series.method_id,
                                                           source_id=series.source_id,
                                                           quality_control_level_id=series.quality_control_level_id).all()
        else:
            q = self._edit_session.query(DataValue).filter_by(
                    site_id=series.site_id,
                    variable_id=series.variable_id,
                    method_id=series.method_id,
                    source_id=series.source_id,
                    quality_control_level_id=series.quality_control_level_id)

        query = q.statement.compile(dialect=self._session_factory.engine.dialect)
        data = pandas.read_sql_query(sql=query, con=self._session_factory.engine,
                                     params=query.params)
        return data

    def get_all_values_df(self):
        """

        :return: Pandas DataFrame object
        """
        q = self._edit_session.query(DataValue).order_by(DataValue.local_date_time)
        query = q.statement.compile(dialect=self._session_factory.engine.dialect)
        data = pandas.read_sql_query(sql=query, con=self._session_factory.engine,
                                     params=query.params)
        columns = list(data)

        columns.insert(0, columns.pop(columns.index("DataValue")))
        columns.insert(1, columns.pop(columns.index("LocalDateTime")))
        columns.insert(2, columns.pop(columns.index("QualifierID")))

        data = data.ix[:, columns]
        return data.set_index(data['LocalDateTime'])

    def get_all_values_list(self):
        """

        :return:
        """
        result = self._edit_session.query(DataValue).order_by(DataValue.local_date_time).all()
        return [x.list_repr() for x in result]

    def get_all_values(self):
        return self._edit_session.query(DataValue).order_by(DataValue.local_date_time).all()

    @staticmethod
    def calcSeason(row):

        month = int(row["Month"])

        if month in [1, 2, 3]:
            return 1
        elif month in [4, 5, 6]:
            return 2
        elif month in [7, 8, 9]:
            return 3
        elif month in [10, 11, 12]:
            return 4

    def get_all_plot_values(self):
        """

        :return:
        """
        q = self._edit_session.query(DataValue.data_value.label('DataValue'),
                                     DataValue.local_date_time.label('LocalDateTime'),
                                     DataValue.censor_code.label('CensorCode'),
                                     func.strftime('%m', DataValue.local_date_time).label('Month'),
                                     func.strftime('%Y', DataValue.local_date_time).label('Year')
                                     # DataValue.local_date_time.strftime('%m'),
                                     # DataValue.local_date_time.strftime('%Y'))
                                     ).order_by(DataValue.local_date_time)
        query = q.statement.compile(dialect=self._session_factory.engine.dialect)
        data = pandas.read_sql_query(sql=query,
                                     con=self._session_factory.engine,
                                     params=query.params)
        data["Season"] = data.apply(self.calcSeason, axis=1)
        return data.set_index(data['LocalDateTime'])

    def get_plot_values(self, seriesID, noDataValue, startDate=None, endDate=None):
        """

        :param seriesID:
        :param noDataValue:
        :param startDate:
        :param endDate:
        :return:
        """
        series = self.get_series_by_id(seriesID)

        DataValues = [
            (dv.data_value, dv.local_date_time, dv.censor_code, dv.local_date_time.strftime('%m'),
             dv.local_date_time.strftime('%Y'))
            for dv in series.data_values
            if dv.data_value != noDataValue if dv.local_date_time >= startDate if dv.local_date_time <= endDate
        ]
        data = pandas.DataFrame(DataValues, columns=["DataValue", "LocalDateTime", "CensorCode", "Month", "Year"])
        data.set_index(data['LocalDateTime'], inplace=True)
        data["Season"] = data.apply(self.calcSeason, axis=1)
        return data

    def get_data_value_by_id(self, id):
        """

        :param id:
        :return:
        """
        try:
            return self._edit_session.query(DataValue).filter_by(id=id).first()
        except:
            return None




            #####################
            #
            # Update functions
            #
            #####################

    def update_series(self, series):
        """

        :param series:
        :return:
        """
        merged_series = self._edit_session.merge(series)
        self._edit_session.add(merged_series)
        self._edit_session.commit()

    def update_dvs(self, dv_list):
        """

        :param dv_list:
        :return:
        """
        merged_dv_list = map(self._edit_session.merge, dv_list)
        self._edit_session.add_all(merged_dv_list)
        self._edit_session.commit()

    #####################
    #
    # Create functions
    #
    #####################
    def save_series(self, series, dvs):
        """ Save to an Existing Series
        :param series:
        :param data_values:
        :return:
        """

        if self.series_exists(series):

            try:
                self._edit_session.add(series)
                self._edit_session.commit()
                self.save_values(dvs)
            except Exception as e:
                self._edit_session.rollback()
                raise e
            logger.info("Existing File was overwritten with new information")
            return True
        else:
            logger.debug("There wasn't an existing file to overwrite, please select 'Save As' first")
            # there wasn't an existing file to overwrite
            raise Exception("Series does not exist, unable to save. Please select 'Save As'")

    def save_new_series(self, series, dvs):
        """ Create as a new catalog entry
        :param series:
        :param data_values:
        :return:
        """
        # Save As case
        if self.series_exists(series):
            msg = "There is already an existing file with this information. Please select 'Save' or 'Save Existing' " \
                  "to overwrite"
            logger.info(msg)
            raise Exception(msg)
        else:
            try:
                self._edit_session.add(series)
                self._edit_session.commit()
                self.save_values(dvs)
                # self._edit_session.add_all(dvs)
            except Exception as e:
                self._edit_session.rollback()
                raise e

        logger.info("A new series was added to the database, series id: " + str(series.id))
        return True

    def save_values(self, values):
        """

        :param values: pandas dataframe
        :return:
        """
        values.to_sql(name="datavalues", if_exists='append', con=self._session_factory.engine, index=False)

    def create_new_series(self, data_values, site_id, variable_id, method_id, source_id, qcl_id):
        """

        :param data_values:
        :param site_id:
        :param variable_id:
        :param method_id:
        :param source_id:
        :param qcl_id:
        :return:
        """
        self.update_dvs(data_values)
        series = Series()
        series.site_id = site_id
        series.variable_id = variable_id
        series.method_id = method_id
        series.source_id = source_id
        series.quality_control_level_id = qcl_id

        self._edit_session.add(series)
        self._edit_session.commit()
        return series

    def create_method(self, description, link):
        """

        :param description:
        :param link:
        :return:
        """
        meth = Method()
        meth.description = description
        if link is not None:
            meth.link = link

        self._edit_session.add(meth)
        self._edit_session.commit()
        return meth

    def create_variable_by_var(self, var):
        """

        :param var:  Variable Object
        :return:
        """
        try:
            self._edit_session.add(var)
            self._edit_session.commit()
            return var
        except:
            return None

    def create_variable(
            self, code, name, speciation, variable_unit_id, sample_medium,
            value_type, is_regular, time_support, time_unit_id, data_type,
            general_category, no_data_value):
        """

        :param code:
        :param name:
        :param speciation:
        :param variable_unit_id:
        :param sample_medium:
        :param value_type:
        :param is_regular:
        :param time_support:
        :param time_unit_id:
        :param data_type:
        :param general_category:
        :param no_data_value:
        :return:
        """
        var = Variable()
        var.code = code
        var.name = name
        var.speciation = speciation
        var.variable_unit_id = variable_unit_id
        var.sample_medium = sample_medium
        var.value_type = value_type
        var.is_regular = is_regular
        var.time_support = time_support
        var.time_unit_id = time_unit_id
        var.data_type = data_type
        var.general_category = general_category
        var.no_data_value = no_data_value

        self._edit_session.add(var)
        self._edit_session.commit()
        return var

    def create_qcl(self, code, definition, explanation):
        """

        :param code:
        :param definition:
        :param explanation:
        :return:
        """
        qcl = QualityControlLevel()
        qcl.code = code
        qcl.definition = definition
        qcl.explanation = explanation

        self._edit_session.add(qcl)
        self._edit_session.commit()
        return qcl

    def create_qualifier_by_qual(self, qualifier):
        self._edit_session.add(qualifier)
        self._edit_session.commit()
        return qualifier

    def create_qualifier(self, code, description):
        """

        :param code:
        :param description:
        :return:
        """
        qual = Qualifier()
        qual.code = code
        qual.description = description

        return self.create_qualifier_by_qual(qual)

    #####################
    #
    # Delete functions
    #
    #####################

    def delete_series(self, series):
        """

        :param series:
        :return:
        """
        try:
            self.delete_values_by_series(series)

            delete_series = self._edit_session.merge(series)
            self._edit_session.delete(delete_series)
            self._edit_session.commit()
        except Exception as e:
            message = "series was not successfully deleted: %s" % e
            print message
            logger.error(message)
            raise e

    def delete_values_by_series(self, series, startdate=None):
        """

        :param series:
        :return:
        """
        try:
            q = self._edit_session.query(DataValue).filter_by(site_id=series.site_id,
                                                              variable_id=series.variable_id,
                                                              method_id=series.method_id,
                                                              source_id=series.source_id,
                                                              quality_control_level_id=series.quality_control_level_id)
            if startdate is not None:
                # start date indicates what day you should start deleting values. the values will delete to the end
                # of the series
                return q.filter(DataValue.local_date_time >= startdate).delete()
            else:
                return q.delete()

        except Exception as ex:
            message = "Values were not successfully deleted: %s" % ex
            print message
            logger.error(message)
            raise ex

    def delete_dvs(self, id_list):
        """

        :param id_list: list of datetimes
        :return:
        """
        try:
            self._edit_session.query(DataValue).filter(DataValue.local_date_time.in_(id_list)).delete(False)
        except Exception as ex:
            message = "Values were not successfully deleted: %s" % ex
            print message
            logger.error(message)
            raise ex

            #####################
            #
            # Exist functions
            #
            #####################

    def series_exists(self, series):
        """

        :param series:
        :return:
        """
        return self.series_exists_quint(
                series.site_id,
                series.variable_id,
                series.method_id,
                series.source_id,
                series.quality_control_level_id
        )

    def series_exists_quint(self, site_id, var_id, method_id, source_id, qcl_id):
        """

        :param site_id:
        :param var_id:
        :param method_id:
        :param source_id:
        :param qcl_id:
        :return:
        """
        try:
            result = self._edit_session.query(Series).filter_by(
                    site_id=site_id,
                    variable_id=var_id,
                    method_id=method_id,
                    source_id=source_id,
                    quality_control_level_id=qcl_id
            ).one()

            return True
        except:
            return False

    def qcl_exists(self, q):
        """

        :param q:
        :return:
        """
        try:
            result = self._edit_session.query(QualityControlLevel).filter_by(code=q.code, definition=q.definition).one()
            return True
        except:

            return False

    def method_exists(self, m):
        """

        :param m:
        :return:
        """
        try:
            result = self._edit_session.query(Method).filter_by(description=m.description).one()
            return True
        except:
            return False

    def variable_exists(self, v):
        """

        :param v:
        :return:
        """
        try:
            result = self._edit_session.query(Variable).filter_by(code=v.code,
                                                                  name=v.name, speciation=v.speciation,
                                                                  variable_unit_id=v.variable_unit_id,
                                                                  sample_medium=v.sample_medium,
                                                                  value_type=v.value_type, is_regular=v.is_regular,
                                                                  time_support=v.time_support,
                                                                  time_unit_id=v.time_unit_id, data_type=v.data_type,
                                                                  general_category=v.general_category,
                                                                  no_data_value=v.no_data_value).one()
            return result
        except:
            return None
