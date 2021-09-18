#data cleaning variables

#province identifiers
province_map = {"Nothern":"Northern",
                "Northern Region" : "Northern",
                "Western Area":'Western'}

drop_province =[ 'Place']
province_col = ['form.health_centre_information.location_information.region_province']

district_col = ['form.health_centre_information.location_information.district']

#define columns which signal location
loc_id = ['number', 
          'form.health_centre_information.facility_name',
          'form.facility_gps',
          'form.health_centre_information.location_information.region_province',
          'form.health_centre_information.location_information.district',
          'form.health_centre_information.location_information.chiefdom',
          'form.health_centre_information.location_information.facility_location',
          'form.health_centre_information.group_number_employed.list_other.other_staff_cadre']

col_ex = ['form.health_centre_information.managing_authority_other',
            'form.health_centre_information.group_number_employed.list_other.other_staff_cadre']