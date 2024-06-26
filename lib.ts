import axios from "axios";
import StreamObject from "stream-json/streamers/StreamObject";
import {stream} from "event-iterator"
import {EventIterator} from "event-iterator/src/event-iterator";
import {Client as PgClient} from 'pg';
import {doMapDeals} from "./map";
import fs from 'fs';
import PQueue from 'p-queue';
import JsonRpcClient from "./JsonRpcClient";
// also https://github.com/filecoin-project/notary-governance/blob/ab71ebed3c4b8cd0a5940d4cae927e5afe787d19/quality-tracking-metrics.md?plain=1#L25
export const createStatement = `CREATE TABLE IF NOT EXISTS current_state (
    deal_id INTEGER NOT NULL PRIMARY KEY,
    piece_cid TEXT NOT NULL,
    piece_size BIGINT NOT NULL,
    verified_deal BOOLEAN NOT NULL,
    client TEXT NOT NULL,
    provider TEXT NOT NULL,
    label TEXT NOT NULL,
    start_epoch INTEGER NOT NULL,
    end_epoch INTEGER NOT NULL,
    storage_price_per_epoch BIGINT NOT NULL,
    provider_collateral BIGINT NOT NULL,
    client_collateral BIGINT NOT NULL,
    sector_start_epoch INTEGER NOT NULL,
    last_updated_epoch INTEGER NOT NULL,
    slash_epoch INTEGER NOT NULL
)`;

export const createClientMappingStatement = `CREATE TABLE IF NOT EXISTS client_mapping (
    client TEXT NOT NULL PRIMARY KEY,
    client_address TEXT NOT NULL
)`;

export const getAllClientMappingStatement = `SELECT client, client_address FROM client_mapping`;

export const insertClientMappingStatement = `INSERT INTO client_mapping (client, client_address) VALUES ($1, $2)`;

export const getAllClientsStatement = `SELECT DISTINCT client FROM current_state`;

export const insertStatementBase = `INSERT INTO current_state (
                               deal_id,
                               piece_cid,
                               piece_size,
                               verified_deal,
                               client,
                               provider,
                               label,
                               start_epoch,
                               end_epoch,
                               storage_price_per_epoch,
                               provider_collateral,
                               client_collateral,
                               sector_start_epoch,
                               last_updated_epoch,
                               slash_epoch) VALUES {values}
                            ON CONFLICT (deal_id) DO UPDATE SET sector_start_epoch = EXCLUDED.sector_start_epoch, last_updated_epoch = EXCLUDED.last_updated_epoch, slash_epoch = EXCLUDED.slash_epoch`;

type DealId = number;
type PieceCid = string;
type PieceSize = bigint;
type VerifiedDeal = boolean;
type Client = string;
type Provider = string;
type Label = string;
type StartEpoch = number;
type EndEpoch = number;
type StoragePricePerEpoch = bigint;
type ProviderCollateral = bigint;
type ClientCollateral = bigint;
type SectorStartEpoch = number;
type LastUpdatedEpoch = number;
type SlashEpoch = number;
type DealRow = [
    DealId,
    PieceCid,
    PieceSize,
    VerifiedDeal,
    Client,
    Provider,
    Label,
    StartEpoch,
    EndEpoch,
    StoragePricePerEpoch,
    ProviderCollateral,
    ClientCollateral,
    SectorStartEpoch,
    LastUpdatedEpoch,
    SlashEpoch
]

interface MarketDeal {
    Proposal: {
        PieceCID: {
            '/': string,
        },
        PieceSize: number,
        VerifiedDeal: boolean,
        Client: string,
        Provider: string,
        Label: string,
        StartEpoch: number,
        EndEpoch: number
        StoragePricePerEpoch: string,
        ProviderCollateral: string,
        ClientCollateral: string,
    }
    ,
    State: {
        SectorStartEpoch: number,
        LastUpdatedEpoch: number,
        SlashEpoch: number,
    },
}

export function getInsertStatement(batch: number): string {
    let j = 1;
    let result = '';
    for (let i = 0; i < batch; i++) {
        result += `($${j++}, $${j++}, $${j++}, $${j++}, $${j++}, $${j++}, $${j++}, $${j++}, $${j++}, $${j++}, $${j++}, $${j++}, $${j++}, $${j++}, $${j++})`;
        if (i < batch - 1) {
            result += ', ';
        }
    }
    return insertStatementBase.replace('{values}', result);
}

export async function* readMarketDealsBatch(path: string, batch: number): AsyncIterable<{ key: string, value: MarketDeal }[]> {
    let list = [];
    for await (const deal of await readMarketDeals(path)) {
        list.push(deal);
        if (list.length >= batch) {
            yield [...list];
            list = [];
        }
    }
    if (list.length > 0) {
        yield [...list];
    }
}

export async function readMarketDeals(path: string): Promise<AsyncIterable<{ key: string, value: MarketDeal }>> {
    console.info('Reading market deals from', path);
    const readStream = fs.createReadStream(path, {autoClose: true});
    readStream.on('error', (err) => {
        console.error(err);
        process.exit(1);
    })
    const p = readStream.pipe(StreamObject.withParser());
    const result = <EventIterator<{ key: string, value: MarketDeal }>>stream.call(p);
    return result;
}

export function epochToTimestamp(epoch: number): number {
    if (epoch === -1) {
        return 0;
    }
    return 1598306400 + epoch * 30;
}

export function convertMarketDeal(deal: {key: string, value: MarketDeal}) : DealRow {
    const { Proposal, State } = deal.value;
    const { PieceCID, PieceSize, VerifiedDeal, Client, Provider, Label, StartEpoch, EndEpoch, StoragePricePerEpoch, ProviderCollateral, ClientCollateral } = Proposal;
    const { SectorStartEpoch, LastUpdatedEpoch, SlashEpoch } = State;
    return [
        parseInt(deal.key),
        PieceCID['/'],
        BigInt(PieceSize),
        VerifiedDeal,
        Client,
        Provider,
        Label,
        StartEpoch,
        EndEpoch,
        BigInt(StoragePricePerEpoch),
        BigInt(ProviderCollateral),
        BigInt(ClientCollateral),
        SectorStartEpoch,
        LastUpdatedEpoch,
        SlashEpoch
    ];
}

const createPieceCidIndex = 'CREATE INDEX IF NOT EXISTS current_state_piece_cid ON current_state (piece_cid)';

const createClientIndex = 'CREATE INDEX IF NOT EXISTS current_state_client ON current_state (client)';

const createProviderIndex = 'CREATE INDEX IF NOT EXISTS current_state_provider ON current_state (provider)';

export async function processDeals(path: string, postgres: PgClient): Promise<void> {
    const queueSize = parseInt(process.env.QUEUE_SIZE || '8');
    const queue = new PQueue({concurrency: queueSize});
    let count = 0;
    let innerCount = 0;
    const batch = parseInt(process.env.BATCH_SIZE || '100');
    const batchInsertStatement = getInsertStatement(batch);
    await postgres.connect();
    try {
        console.info(createStatement);
        await postgres.query(createStatement);
        console.info(createClientMappingStatement);
        await postgres.query(createClientMappingStatement);
        await postgres.query(createPieceCidIndex);
        await postgres.query(createClientIndex);
        await postgres.query(createProviderIndex);
        const allClients: string[] = (await postgres.query(getAllClientsStatement)).rows.map(row => row.client);
        const clientMappingRows: { client: string, client_address: string }[] = (await postgres.query(getAllClientMappingStatement)).rows;
        const clientMapping = new Map<string, string>();
        for (const row of clientMappingRows) {
            clientMapping.set(row.client, row.client_address);
        }
        const newClients = new Set<string>();
        for (const client of allClients) {
            if (!clientMapping.has(client)) {
                newClients.add(client);
            }
        }

        for await (const marketDeal of await readMarketDealsBatch(path, batch)) {
            for(const deal of marketDeal) {
                const client = deal.value.Proposal.Client;
                if (!clientMapping.has(client)) {
                    newClients.add(client);
                }
            }
            await queue.onEmpty();
            queue.add(async () => {
                try {
                    if (marketDeal.length === batch) {
                        await postgres.query({
                            name: 'insert-new-deal-batch',
                            text: batchInsertStatement,
                            values: marketDeal.map(convertMarketDeal).flat()
                        });
                    } else {
                        await postgres.query({
                            text: getInsertStatement(marketDeal.length),
                            values: marketDeal.map(convertMarketDeal).flat()
                        });
                    }
                } catch (e) {
                    console.error(e);
                    console.error(`Failed to insert from ${marketDeal[0].key} to ${marketDeal[marketDeal.length - 1].key}`);
                }
                count += marketDeal.length;
                innerCount += marketDeal.length;
                if (innerCount >= 10000) {
                    innerCount -= 10000;
                    console.log(`Processed ${count} deals`);
                }
            });
        }
        const jsonAuth = process.env.GLIF_AUTH;
        if (!jsonAuth) {
            console.error('GLIF_AUTH environment variable is not set');
            process.exit(1);
        }

        const jsonRpcClient = new JsonRpcClient('https://api.node.glif.io/rpc/v0', 'Filecoin.', {
            headers: { Authorization: `Bearer ${jsonAuth}` }
        });
        for (const client of newClients) {
            await queue.onEmpty();
            queue.add(async () => {
                try {
                    const result = await jsonRpcClient.call('StateAccountKey', [client, null]);
                    if (!result.error && result.result) {
                        const address = result.result;
                        console.log(`Adding new client mapping ${client} -> ${address}`);
                        await postgres.query({
                            text: insertClientMappingStatement,
                            values: [client, address]
                        });
                    }
                } catch (e) {
                    console.error(e);
                    console.error(`Failed to add new client mapping for ${client}`);
                }
            });
        }
        await queue.onIdle();
    } finally {
        await postgres.end();
    }
    console.log('Total processed', count, 'deals');
}

export async function handler() {
    const url = process.env.INPUT_FILE || 'StateMarketDeals.json';
    console.log({
        BATCH_SIZE: process.env.BATCH_SIZE,
        QUEUE_SIZE: process.env.QUEUE_SIZE,
        PGHOST: process.env.PGHOST,
        PGPORT: process.env.PGPORT,
        PGUSER: process.env.PGUSER,
        PGDATABASE: process.env.PGDATABASE,
        POLL_CONNECTION_TIMEOUT: process.env.POLL_CONNECTION_TIMEOUT,
        INPUT_URL: process.env.INPUT_URL,
        url: url
    });
    const postgres = new PgClient({
        connectionTimeoutMillis: parseInt(process.env.POLL_CONNECTION_TIMEOUT || '0'),
    });
    if (process.env.MAP) {
        await doMapDeals(url, postgres);
    } else {
        await processDeals(url, postgres);
    }
    const response = {
        statusCode: 200
    };
    return response;
}
