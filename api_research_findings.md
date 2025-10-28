# 99acres Property API Research Findings

## API Parameters Analysis

From the components data, I found these parameters for property API calls:

### Rental Properties (`preference: R`)
```
preference: R
platform: DESKTOP
algoType: RESALE_RENTAL
src: RESALE_RENTAL_XID
lazy: True
building_id: 208597
city: 217
res_com: R
configurationRequired: True
configurations: 0_0
noxid: Y
page_size: 2
```

### Resale Properties (`preference: S`)
```
preference: S
platform: DESKTOP
algoType: RESALE_RENTAL
src: RESALE_RENTAL_XID
lazy: True
building_id: 208597
city: 217
res_com: R
configurationRequired: True
configurations: 0_0
noxid: Y
page_size: 2
transact_type: 1
```

## API Endpoint Discovery

Based on common 99acres patterns, the API endpoints are likely:
- Base: `https://www.99acres.com/api/v1/`
- Search endpoint: `/search` or `/properties`
- Parameters passed as query string

## Next Steps
1. Test API endpoint construction
2. Make test API calls to verify response format
3. Identify the correct endpoint URL pattern