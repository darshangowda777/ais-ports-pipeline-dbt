import pandas as pd

INPUT  = "data/UpdatedPub150.csv"
OUTPUT = "data/world_port_index_final.csv"

df = pd.read_csv(INPUT, low_memory=False)
print(f"Raw: {len(df)} rows, {len(df.columns)} columns")

# Select 13 meaningful columns (dropped 96 — see comments below)
# DROPPED: Maximum Vessel Draft (76% zeros), all facilities/cranes/services
# (mostly Unknown), nautical chart IDs, vessel length/beam (90%+ zeros)
df_clean = df[[
    'World Port Index Number',
    'UN/LOCODE',
    'Main Port Name',
    'Country Code',
    'Region Name',
    'World Water Body',
    'Latitude',
    'Longitude',
    'Harbor Size',
    'Harbor Type',
    'Harbor Use',
    'Shelter Afforded',
    'Channel Depth (m)',
    'Cargo Pier Depth (m)',
]].copy()

df_clean.columns = [
    'wpi_number','locode','port_name','country_code','region_raw',
    'water_body','latitude','longitude','harbor_size','harbor_type',
    'harbor_use','shelter_quality','channel_depth_m','cargo_pier_depth_m',
]

# Build port_id from UN/LOCODE (87.9% coverage), fallback to WPI number
df_clean['locode'] = df_clean['locode'].astype(str).str.strip()
df_clean['port_id'] = df_clean.apply(
    lambda r: r['locode'].replace(' ', '')
    if r['locode'] not in ['', 'nan']
    else f"WPI{int(r['wpi_number'])}", axis=1
)

# Clean region — strip number suffix e.g. 'United States E Coast -- 6585'
df_clean['region'] = (
    df_clean['region_raw'].astype(str)
    .str.split(' -- ').str[0].str.strip()
)
df_clean['region'] = df_clean['region'].replace(['nan','','null'], None)
df_clean['region'] = df_clean['region'].fillna(df_clean['country_code'])

# Clean string columns
for col in ['port_name','country_code','water_body',
            'harbor_size','harbor_type','harbor_use','shelter_quality']:
    df_clean[col] = df_clean[col].astype(str).str.strip()
    df_clean[col] = df_clean[col].replace(['nan','',' '], 'Unknown')

# Clean numeric columns
for col in ['latitude','longitude','channel_depth_m','cargo_pier_depth_m']:
    df_clean[col] = pd.to_numeric(df_clean[col], errors='coerce')

# Drop rows missing coordinates (essential for Spark geofence join)
before = len(df_clean)
df_clean = df_clean.dropna(subset=['latitude','longitude'])
print(f"Dropped {before - len(df_clean)} rows with missing coordinates")

# Final column order
df_final = df_clean[[
    'port_id','port_name','country_code','region','water_body',
    'latitude','longitude','harbor_size','harbor_type','harbor_use',
    'shelter_quality','channel_depth_m','cargo_pier_depth_m'
]]

df_final.to_csv(OUTPUT, index=False)

locode_count = (~df_final['port_id'].str.startswith('WPI')).sum()
wpi_count    = df_final['port_id'].str.startswith('WPI').sum()

print(f"\n✅ Saved → {OUTPUT}")
print(f"   Total ports     : {len(df_final)}")
print(f"   Columns         : {len(df_final.columns)}")
print(f"   UN/LOCODE ports : {locode_count}")
print(f"   WPI fallback    : {wpi_count}")
print(f"   Null regions    : {df_final['region'].isna().sum()}")
print(f"\nColumn null summary:")
for col in df_final.columns:
    print(f"   {col:<25} nulls: {df_final[col].isna().sum()}")
print(f"\nSample:")
print(df_final[['port_id','port_name','country_code','region','channel_depth_m']].head(8).to_string())
