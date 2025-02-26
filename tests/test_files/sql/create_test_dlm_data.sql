-- Insert into location (you'll need to populate this first for foreign key constraints)
INSERT INTO location (location_name, location_type, location_country) VALUES
('SKA Site 1', 'Site', 'South Africa'),
('SKA Site 2', 'Site', 'Australia');

-- Insert into storage (populate location_id with values from the location table)
INSERT INTO storage (location_id, storage_name, storage_type, storage_interface) VALUES
((SELECT location_id FROM location WHERE location_name = 'SKA Site 1'), 'Storage 1A', 'HDD', 'SATA'),
((SELECT location_id FROM location WHERE location_name = 'SKA Site 2'), 'Storage 2B', 'SSD', 'NVMe');


-- Insert test data into data_item with 'execution_block' in metadata
INSERT INTO data_item (item_name, storage_id, item_size, item_checksum, item_type, item_format, metadata) VALUES
('file1.fits', (SELECT storage_id FROM storage WHERE storage_name = 'Storage 1A'), 1024, 'checksum123', 'file', 'FITS', '{"instrument": "MeerKAT", "telescope": "SKA-Mid", "execution_block": "eb-dlmtest1-20240219-00001"}'),
('file2.txt', (SELECT storage_id FROM storage WHERE storage_name = 'Storage 1A'), 512, 'checksum456', 'file', 'TXT', '{"author": "John Doe", "date": "2024-07-26", "execution_block": "eb-dlmtest2-20240219-00001"}'),
('file3.csv', (SELECT storage_id FROM storage WHERE storage_name = 'Storage 2B'), 2048, 'checksum789', 'file', 'CSV', '{"project": "Galaxy Survey", "data_type": "spectral", "execution_block": "eb-dlmtest3-20240219-00001"}'),
('file4.json', (SELECT storage_id FROM storage WHERE storage_name = 'Storage 2B'), 1024, 'checksumabc', 'file', 'JSON', '{"status": "processed", "level": "L1", "execution_block": "eb-dlmtest4-20240219-00001"}');

-- Example with more fields and NULLs
INSERT INTO data_item (item_name, storage_id, item_size, item_checksum, item_type, item_format, metadata, item_level, item_phase, item_state, UID_expiration, item_owner, parents)
VALUES ('file5.hdf5', (SELECT storage_id FROM storage WHERE storage_name = 'Storage 1A'), 4096, 'checksumdef', 'file', 'HDF5', '{"version": "1.0", "description": "Simulation data", "execution_block": "EB005"}', 2, 'CALIBRATED', 'COMPLETED', NOW() + INTERVAL '7 days', 'Jane Doe', (SELECT UID FROM data_item WHERE item_name = 'file1.fits'));


-- Insert more data with variations
INSERT INTO data_item (item_name, storage_id, item_size, item_checksum, item_type, item_format, metadata, item_level, item_phase, item_state, UID_expiration, item_owner)
VALUES
('file6.fits', (SELECT storage_id FROM storage WHERE storage_name = 'Storage 2B'), 2048, 'checksumghi', 'file', 'FITS', '{"instrument": "ASKAP", "telescope": "SKA-Low", "execution_block": "EB006"}', 1, 'RAW', 'INITIALIZED', NOW() + INTERVAL '30 days', 'SKA'),
('file7.txt', (SELECT storage_id FROM storage WHERE storage_name = 'Storage 1A'), 1024, 'checksumjkl', 'file', 'TXT', '{"author": "Alice Smith", "date": "2024-07-27", "execution_block": "EB007"}', 0, 'GAS', 'PROCESSING', NOW() + INTERVAL '90 days', 'SKA');


-- Verify the data
SELECT * FROM data_item;

-- Query to check if execution_block exists and its value
SELECT *
FROM data_item
-- WHERE metadata ? 'execution_block';  -- Check if the "execution_block" key exists