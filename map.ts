import PQueue from 'p-queue';
import {Client as PgClient} from 'pg';

export const cmTableSetupStatement = `
create table if not exists github_client_mapping
(
    client_address text constraint github_client_mapping_pk unique,
    unique_id      text
)
`;
export const dtTableSetupStatement = `
create table if not exists deal_tags
(
    deal_id            integer not null primary key,
    cid_overreplicated boolean default false,
    cid_shared         boolean default false,
    cid_unique         boolean default false,
    sector_start       timestamp,
    piece_size         bigint
)`;

export const setupStatement = `CREATE OR REPLACE FUNCTION epoch_to_timestamp(epoch INTEGER)
RETURNS INTEGER AS $$
BEGIN
RETURN epoch * 30 + 1598306400;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION tag_over_replicated_deals()
RETURNS VOID AS $$
BEGIN
INSERT INTO deal_tags (deal_id, cid_overreplicated, sector_start, piece_size)
SELECT
    cs.deal_id,
    cs.row_number > 1 AS cid_overreplicated,
    TO_TIMESTAMP(epoch_to_timestamp(cs.sector_start_epoch)),
    cs.piece_size
FROM (
         SELECT
             *,
             ROW_NUMBER() OVER (
                 PARTITION BY piece_cid, provider
                 ORDER BY sector_start_epoch
                 ) AS row_number
         FROM
             current_state
         WHERE
                 verified_deal = true
           AND sector_start_epoch > 0
     ) AS cs
ON CONFLICT (deal_id)
    DO UPDATE SET cid_overreplicated = EXCLUDED.cid_overreplicated,
                  sector_start = EXCLUDED.sector_start, piece_size = EXCLUDED.piece_size;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION tag_cid_sharing_deals()
RETURNS VOID AS $$
BEGIN
WITH owner_count AS (
    SELECT
        piece_cid,
        COUNT(DISTINCT owner) AS owner_count
    FROM (
             SELECT
                 current_state.piece_cid,
                 COALESCE(github_client_mapping.unique_id::TEXT, current_state.client) AS owner
             FROM
                 current_state
                     LEFT JOIN github_client_mapping ON current_state.client = github_client_mapping.client_address
             WHERE
                     verified_deal = true
               AND sector_start_epoch > 0
         ) AS distinct_owners
    GROUP BY piece_cid
)
INSERT INTO deal_tags (deal_id, cid_shared, sector_start, piece_size)
SELECT
    cs.deal_id,
    (cs.row_number > 1 AND oc.owner_count > 1) AS cid_shared,
    TO_TIMESTAMP(epoch_to_timestamp(cs.sector_start_epoch)),
    cs.piece_size
FROM (
         SELECT
             cs_with_owner.*,
             ROW_NUMBER() OVER (
                 PARTITION BY piece_cid, owner
                 ORDER BY sector_start_epoch
                 ) AS row_number
         FROM (
                  SELECT
                      current_state.*,
                      COALESCE(github_client_mapping.unique_id::TEXT, current_state.client) AS owner
                  FROM
                      current_state
                          LEFT JOIN github_client_mapping ON current_state.client = github_client_mapping.client_address
                  WHERE
                          verified_deal = true
                    AND sector_start_epoch > 0
              ) AS cs_with_owner
     ) AS cs
         JOIN owner_count oc ON cs.piece_cid = oc.piece_cid
ON CONFLICT (deal_id)
    DO UPDATE SET cid_shared = EXCLUDED.cid_shared,
                  sector_start = EXCLUDED.sector_start, piece_size = EXCLUDED.piece_size;
END;
$$ LANGUAGE plpgsql;


CREATE OR REPLACE FUNCTION tag_cid_unique_deals()
RETURNS VOID AS $$
BEGIN
WITH eligible_clients AS (
    SELECT
        COALESCE(gcm.unique_id::TEXT, cs.client) AS grouped_client,
        SUM(cs.piece_size) AS total_storage,
        MAX(cs.sector_start_epoch) AS max_sector_start_epoch
    FROM
        current_state cs
            LEFT JOIN github_client_mapping gcm ON cs.client = gcm.client_address
    WHERE
            cs.verified_deal = true
      AND cs.sector_start_epoch > 0
    GROUP BY
        grouped_client
    HAVING
            SUM(cs.piece_size) > (1::BIGINT * 1024 * 1024 * 1024 * 1024)
       AND (TO_TIMESTAMP(epoch_to_timestamp(MAX(cs.sector_start_epoch))) + interval '6 weeks') <= NOW()
),
     unique_deals AS (
         SELECT
             cs.*,
             COALESCE(gcm.unique_id::TEXT, cs.client) AS grouped_client,
             ROW_NUMBER() OVER (
                 PARTITION BY COALESCE(gcm.unique_id::TEXT, cs.client), cs.piece_cid
                 ORDER BY cs.sector_start_epoch
                 ) AS row_number
         FROM
             current_state cs
                 LEFT JOIN github_client_mapping gcm ON cs.client = gcm.client_address
         WHERE
                 cs.verified_deal = true
           AND cs.sector_start_epoch > 0
     )
INSERT INTO deal_tags (deal_id, cid_unique, sector_start, piece_size)
SELECT
    ud.deal_id,
    (ud.row_number = 1 AND ec.grouped_client IS NOT NULL) AS cid_unique,
    TO_TIMESTAMP(epoch_to_timestamp(ud.sector_start_epoch)),
    ud.piece_size
FROM
    unique_deals ud
        LEFT JOIN eligible_clients ec ON ud.grouped_client = ec.grouped_client
ON CONFLICT (deal_id)
    DO UPDATE SET
                  cid_unique = EXCLUDED.cid_unique,
                  sector_start = EXCLUDED.sector_start,
                  piece_size = EXCLUDED.piece_size;
END;
$$ LANGUAGE plpgsql`;

export async function doMapDeals(path: string, postgres: PgClient): Promise<void> {
    const queueSize = parseInt(process.env.QUEUE_SIZE || '8');
    const queue = new PQueue({concurrency: queueSize});
    let count = 0;
    let innerCount = 0;
    const batch = parseInt(process.env.BATCH_SIZE || '100');
    await postgres.connect();
    try {
        console.info(cmTableSetupStatement);
        await postgres.query(cmTableSetupStatement);
        console.info(dtTableSetupStatement);
        await postgres.query(dtTableSetupStatement);
        console.info(setupStatement);
        await postgres.query(setupStatement);
        console.info('running tag_over_replicated_deals()')
        await postgres.query('SELECT tag_over_replicated_deals()');
        console.info('running tag_cid_sharing_deals()')
        await postgres.query('SELECT tag_cid_sharing_deals()');
        console.info('running tag_cid_unique_deals()')
        await postgres.query('SELECT tag_cid_unique_deals()');
    } finally {
        await postgres.end();
    }
    console.log('Total processed', count, 'deals');
}