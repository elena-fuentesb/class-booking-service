
CREATE TABLE IF NOT EXISTS public.class_occupancy (
    classid VARCHAR(255) NOT NULL,
    count BIGINT NOT NULL,
    PRIMARY KEY (classid));
