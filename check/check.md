i am giong to updata address

db: address_db
collection: country_status, address

algorithm

while not find country_status == "completed"
    select one country country_status == "completed" -> set status "checking"
    get all count address with selected country from address collection
        get all address with country but loop (skip, limit=1000) for memory secure.
        loop address and check: send fulladdress and selected country namd-> test.py validate_address_region
            if true: continue
            else : send fulladdress -> nominatim search -> res -> updata address fulladdress: display_name, city, country, street
    set status "checked"