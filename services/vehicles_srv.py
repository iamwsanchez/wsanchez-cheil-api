import polars as pl

def vehicles_std():   
    lf = pl.scan_csv('./data/vehicle.csv')  
    lf.sink_parquet('./data/vehicles.parquet')
    lf = pl.scan_parquet('./data/vehicles.parquet')
    
    vh_list_std = list(lf.groupby('class').agg(
    pl.std('compactness').round(0),
    pl.std('circularity').round(0),
    pl.std('distance_circularity').round(0),
    pl.std('radius_ratio').round(0),
    pl.std('pr.axis_aspect_ratio').round(0).alias('pr_axis_aspect_ratio'),
    pl.std('max.length_aspect_ratio').round(0).alias('max_length_aspect_ratio'),
    pl.std('scatter_ratio').round(0),
    pl.std('elongatedness').round(0),
    pl.std('pr.axis_rectangularity').round(0).alias('pr_axis_rectangularity'),
    pl.std('max.length_rectangularity').round(0).alias('max_length_rectangularity'),
    pl.std('scaled_variance').round(0),
    pl.std('scaled_variance.1').round(0).alias('scaled_variance_1'),
    pl.std('scaled_radius_of_gyration').round(0),
    pl.std('scaled_radius_of_gyration.1').round(0).alias('scaled_radius_of_gyration_1'),
    pl.std('skewness_about').round(0),
    pl.std('skewness_about.1').round(0).alias('skewness_about_1'),
    pl.std('skewness_about.2').round(0).alias('skewness_about_2'),
    pl.std('hollows_ratio').round(0)).collect().to_dicts())
    
    return vh_list_std

def vehicles_mean():   
    lf = pl.scan_csv('./data/vehicle.csv')  
    lf.sink_parquet('./data/vehicles.parquet')
    lf = pl.scan_parquet('./data/vehicles.parquet')
    
    vh_list_mean= list(lf.groupby('class').agg(
    pl.mean('compactness').round(0),
    pl.mean('circularity').round(0),
    pl.mean('distance_circularity').round(0),
    pl.mean('radius_ratio').round(0),
    pl.mean('pr.axis_aspect_ratio').round(0).alias('pr_axis_aspect_ratio'),
    pl.mean('max.length_aspect_ratio').round(0).alias('max_length_aspect_ratio'),
    pl.mean('scatter_ratio').round(0),
    pl.mean('elongatedness').round(0),
    pl.mean('pr.axis_rectangularity').round(0).alias('pr_axis_rectangularity'),
    pl.mean('max.length_rectangularity').round(0).alias('max_length_rectangularity'),
    pl.mean('scaled_variance').round(0),
    pl.mean('scaled_variance.1').round(0).alias('scaled_variance_1'),
    pl.mean('scaled_radius_of_gyration').round(0),
    pl.mean('scaled_radius_of_gyration.1').round(0).alias('scaled_radius_of_gyration_1'),
    pl.mean('skewness_about').round(0),
    pl.mean('skewness_about.1').round(0).alias('skewness_about_1'),
    pl.mean('skewness_about.2').round(0).alias('skewness_about_2'),
    pl.mean('hollows_ratio').round(0)).collect().to_dicts())
    
    return vh_list_mean