from Utilities.H2OServices import H2OService

service = H2OService()
service.LoadData()

print('- Starting operation')
for resource_id, managed_resource in service.ManagedResources.iteritems():
    print('- Working with resource {}'.format(resource_id))
    selected_series = managed_resource.selected_series
    managed_resource.selected_series = {}
    for old_series_id, series in selected_series.iteritems():
        print('-- Changing series id {} for new series id {}'.format(old_series_id, series.odm_id))
        managed_resource.selected_series[series.odm_id] = series
        series.SeriesID = series.odm_id

print('- Writing new operations file with new series ids')
service.SaveData()
print('DONE!')
