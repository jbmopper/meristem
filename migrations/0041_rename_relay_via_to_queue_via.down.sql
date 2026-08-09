-- Reverse the expand by dropping the added column. relay_via was never
-- dropped and the projectors kept writing it, so no data is lost.

ALTER TABLE nodes DROP COLUMN queue_via;
