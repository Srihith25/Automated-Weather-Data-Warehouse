-- Function to populate date dimension
CREATE OR REPLACE FUNCTION populate_date_dimension(start_date DATE, end_date DATE)
RETURNS void AS $$
BEGIN
    INSERT INTO dim_date (
        full_date, year, quarter, month, month_name,
        week_of_year, day_of_month, day_of_week, day_name, is_weekend
    )
    SELECT 
        date_series::DATE,
        EXTRACT(YEAR FROM date_series),
        EXTRACT(QUARTER FROM date_series),
        EXTRACT(MONTH FROM date_series),
        TO_CHAR(date_series, 'Month'),
        EXTRACT(WEEK FROM date_series),
        EXTRACT(DAY FROM date_series),
        EXTRACT(DOW FROM date_series),
        TO_CHAR(date_series, 'Day'),
        EXTRACT(DOW FROM date_series) IN (0, 6)
    FROM generate_series(start_date, end_date, '1 day'::interval) date_series
    ON CONFLICT (full_date) DO NOTHING;
END;
$$ LANGUAGE plpgsql;

-- Populate date dimension for 2024-2026
SELECT populate_date_dimension('2024-01-01'::DATE, '2026-12-31'::DATE);

-- Function to get or create dimension IDs
CREATE OR REPLACE FUNCTION get_or_create_date_id(p_date DATE)
RETURNS INT AS $$
DECLARE
    v_date_id INT;
BEGIN
    -- Check if date exists
    SELECT date_id INTO v_date_id
    FROM dim_date
    WHERE full_date = p_date;
    
    -- If not exists, insert it
    IF v_date_id IS NULL THEN
        INSERT INTO dim_date (
            full_date, year, quarter, month, month_name,
            week_of_year, day_of_month, day_of_week, day_name, is_weekend
        )
        VALUES (
            p_date,
            EXTRACT(YEAR FROM p_date),
            EXTRACT(QUARTER FROM p_date),
            EXTRACT(MONTH FROM p_date),
            TO_CHAR(p_date, 'Month'),
            EXTRACT(WEEK FROM p_date),
            EXTRACT(DAY FROM p_date),
            EXTRACT(DOW FROM p_date),
            TO_CHAR(p_date, 'Day'),
            EXTRACT(DOW FROM p_date) IN (0, 6)
        )
        RETURNING date_id INTO v_date_id;
    END IF;
    
    RETURN v_date_id;
END;
$$ LANGUAGE plpgsql;