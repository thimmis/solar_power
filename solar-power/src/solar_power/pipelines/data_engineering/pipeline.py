from kedro.pipeline import Pipeline, node
from .nodes import wea_gen_merge, merge_stations_data,\
     data_sampling_methods, extract_skey_list,\
     write_to_catalog
from kedro.framework import context

def create_pipeline(**kwargs):
    
    stations = ['1','2']
    my_pipe = []
    
    for station in stations:
        my_pipe.extend(
            [
                node(
                func=wea_gen_merge,
                inputs=[f'gen{station}_p',f'gen{station}_w'],
                outputs=f'station_{station}',
                name = f'preprocess_{station}_node'
                ),
            ]
        )

    my_pipe.extend(
        [
            node(
                func=merge_stations_data,
                inputs=[f'station_{station}' for station in stations],
                outputs='all_stations',
                name='stations_concat_node'
            ),
        ]
    )

    my_pipe.extend(
        [
            node(
                func=extract_skey_list,
                inputs='all_stations',
                outputs=['source_keys','station_ids'],
                name='extracting_keys_and_ids_node'
            ),
        ]
    )

    my_pipe.extend(
        [
            node(
                func=data_sampling_methods,
                inputs='all_stations',
                outputs=[
                    'daily_yield_ind',
                    'daily_yield_stns',
                    'daily_yield_all',
                    'ac_power_ind',
                    'ac_power_all'],
                name='data_sampling_node'
            ),
        ]
    )

    my_pipe.extend(
        [
            node(
                func=write_to_catalog,
                inputs=['base_cat','source_keys','station_ids'],
                outputs='revised_cat',
                name='updating_catalog_node'
            ),
        ]
    )
        
    
    return Pipeline(
        my_pipe
    )