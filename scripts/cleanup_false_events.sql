-- Cleanup false OZON_ITEM_ADD / OZON_ITEM_REMOVE events
-- These are caused by:
--   1. API intermittent 400 errors → empty products → false REMOVE
--   2. Campaign STOPPED→RUNNING transitions → re-tracking as ADD
--   3. Docker restart/deploy → Redis state lost → all items seen as "new"
--
-- Pattern: same (shop_id, advert_id, nm_id) has ADD + REMOVE
-- within a short timeframe (1-2 hours apart)
--
-- RUN THIS ONLY AFTER DEPLOYING THE FIX!
-- The fix prevents new false events from being generated.

-- 1. Preview: how many false events exist
SELECT
    shop_id,
    COUNT(*) as total_false_events
FROM event_log e1
WHERE event_type IN ('OZON_ITEM_ADD', 'OZON_ITEM_REMOVE')
AND EXISTS (
    SELECT 1 FROM event_log e2
    WHERE e2.shop_id = e1.shop_id
    AND e2.advert_id = e1.advert_id
    AND e2.nm_id = e1.nm_id
    AND e2.event_type = CASE
        WHEN e1.event_type = 'OZON_ITEM_ADD' THEN 'OZON_ITEM_REMOVE'
        ELSE 'OZON_ITEM_ADD'
    END
    AND ABS(EXTRACT(EPOCH FROM (e2.created_at - e1.created_at))) < 7200  -- within 2 hours
)
GROUP BY shop_id;

-- 2. Delete false events (paired ADD+REMOVE within 2 hours)
DELETE FROM event_log
WHERE id IN (
    SELECT e1.id
    FROM event_log e1
    WHERE e1.event_type IN ('OZON_ITEM_ADD', 'OZON_ITEM_REMOVE')
    AND EXISTS (
        SELECT 1 FROM event_log e2
        WHERE e2.shop_id = e1.shop_id
        AND e2.advert_id = e1.advert_id
        AND e2.nm_id = e1.nm_id
        AND e2.event_type = CASE
            WHEN e1.event_type = 'OZON_ITEM_ADD' THEN 'OZON_ITEM_REMOVE'
            ELSE 'OZON_ITEM_ADD'
        END
        AND ABS(EXTRACT(EPOCH FROM (e2.created_at - e1.created_at))) < 7200
    )
);
