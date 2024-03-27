CREATE TABLE public.users (
    id               INTEGER IDENTITY(1,1) PRIMARY KEY,
    user_id          VARCHAR(50) NOT NULL,
    user_attributes  SUPER,
    user_created     TIMESTAMP DEFAULT SYSDATE
);

CREATE TABLE public.events (   
    id                INTEGER IDENTITY(1,1) PRIMARY KEY,
    event_name        VARCHAR(255),
    user_id           VARCHAR(50) REFERENCES Users(id),
    event_attributes  SUPER, -- Use SUPER data type for JSON
    event_created     TIMESTAMP DEFAULT SYSDATE
);

CREATE FUNCTION attr_validate (filter_obj VARCHAR(max), attributes VARCHAR(max))
  returns bool
stable
as $$
    import json
    parsed_filter_obj = json.loads(filter_obj)
    parsed_attributes = json.loads(attributes)

    for attribute in parsed_filter_obj:
        if (
            attribute not in parsed_attributes
            or parsed_attributes[attribute] not in parsed_filter_obj[attribute]
        ):
            return False

    return True
$$ language plpythonu;
