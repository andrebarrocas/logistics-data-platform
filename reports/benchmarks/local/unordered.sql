SELECT c.carrier_name, sum(f.amount_minor)::BIGINT amount_minor,
        count(*)::BIGINT shipments FROM events_unordered f JOIN carriers c USING(carrier_key)
        WHERE service_date >= DATE '2025-06-01' AND service_date < DATE '2025-06-08'
        GROUP BY c.carrier_name ORDER BY c.carrier_name;
