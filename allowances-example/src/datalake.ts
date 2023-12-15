import * as sdk from '@parsiq/datalake-sdk';
import { TsunamiApiClient, ChainId } from '@parsiq/tsunami-client';
import { TsunamiFilter, TsunamiEvent, DatalakeTsunamiApiClient } from '@parsiq/tsunami-client-sdk-http';
import { Interface } from '@ethersproject/abi';
import * as util from 'util';

// Import your ABIs here, in a format suitable for decoding using @ethersproject/abi.
import ERC20Abi from './WETHAbi.json';

// Put your Tsunami API key here.
const TSUNAMI_API_KEY = 'YOUR_API_KEY';
// This is the chain ID for Ethereum mainnet Tsunami API. Change it if you want to work with a different net.
const TSUNAMI_API_NET = ChainId.ETH_SEPOLIA;

// The address of the wallet that you want to monitor for transfers and approvals.
const WALLET_ADDRESS = '0xE67ddd0Ef25BC9d6A2A55b4b5946140B9e570121';
// USDT contract address for starters.
const CONTRACT_ADDRESS = '0xfFf9976782d46CC05630D1f6eBAb18b2324d6B14';
const CONTRACT_ADDRESS_LOWERCASE = '0xfff9976782d46cc05630d1f6ebab18b2324d6b14';

// topic_0 hashes of our events of interest.
const TRANSFER_EVENT_TOPIC_0 = '0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef';
const APPROVAL_EVENT_TOPIC_0 = '0x8c5be1e5ebec7d5bd14f71427d1e84f3dd0314c0f7b2291e5b200ac8c7c3b925';
const WETH_DEPOSIT_EVENT_TOPIC_0 = '0xe1fffcc4923d04b559f4d29a8bfc6cda04eb5b0d3c460751c2402c5c5cc9109c';

// sig hashes of calls of interest.
const TRANSFER_FROM_SIG_HASH = '0x23b872dd';

// The block from which you want to monitor the wallet.
const MONITORING_START_BLOCK_NUMBER = 4_567_462;

// This defines the layout of the type-safe K-V storage.
type DatalakeStorageLayout = {
    '': any,
    balances: { address: string, balance: number },
    allowances: { owner_spender: string, allowance: number },
    transfers: { from: string, to: string, value: number },
    transferFroms: { owner: string, spender: string, value: number },
    approvals: { owner: string, spender: string, value: number },
    deposits: { to: string, value: number },
};

type DatalakeStorageMetaLayout = {
    '': {},
    balances: {},
    allowances: {},
    transfers: {},
    transferFroms: {},
    approvals: {},
    deposits: {},
};

// Types for decoded events follow.
type TransferEvent = {
    src: string,
    dst: string,
    wad: number,
}

type ApprovalEvent = {
    src: string,
    guy: string,
    wad: number,
}

type DepositEvent = {
    dst: string,
    wad: number,
}

class Datalake extends sdk.AbstractMultiStorageDataLakeBase<DatalakeStorageLayout, DatalakeStorageMetaLayout, TsunamiFilter, TsunamiEvent> {
    private ERC20Decoder: Interface;

    // Construct ABI decoders here.
    constructor() {
        super();
        this.ERC20Decoder = new Interface(ERC20Abi);
    }

    public override getProperties(): sdk.DataLakeProperties {
        return {
            id: 'DATALAKE-TEMPLATE',
            initialBlockNumber: MONITORING_START_BLOCK_NUMBER,
        };
    }

    // This method generates the filter used to retrieve events from Tsunami. Filter may change from block to block.
    public async genTsunamiFilterForBlock(block: sdk.Block & sdk.DataLakeRunnerState, isNewBlock: boolean): Promise<TsunamiFilter> {
        return {
            contract: [CONTRACT_ADDRESS],
            topic_0: [TRANSFER_EVENT_TOPIC_0, APPROVAL_EVENT_TOPIC_0, WETH_DEPOSIT_EVENT_TOPIC_0],
        };
    }

    // Main event handler. (Not the UFC main event though.)
    public async processTsunamiEvent(event: TsunamiEvent & sdk.TimecodedEvent & sdk.DataLakeRunnerState): Promise<void | TsunamiFilter> {
        switch (event.topic_0) {
            case TRANSFER_EVENT_TOPIC_0:
                await this.processTransferEvent(event);
                break;
            case APPROVAL_EVENT_TOPIC_0:
                await this.processApprovalEvent(event);
                break;
            case WETH_DEPOSIT_EVENT_TOPIC_0:
                await this.processDepositEvent(event);
                break;
        }
    }

    private async processTransferEvent(event: TsunamiEvent): Promise<void> {
        // Decodes the event.
        const fragment = this.ERC20Decoder.getEvent(event.topic_0!);
        const decoded = this.ERC20Decoder.decodeEventLog(fragment, event.log_data!, [
            event.topic_0!,
            event.topic_1!,
            event.topic_2!
        ]) as unknown as TransferEvent;
        
        // Checks whether the event is relevant to the wallet we are monitoring.
        this.log('INFO', util.inspect(decoded));
        const from = decoded.src.toString();
        const to = decoded.dst.toString();
        if (from === WALLET_ADDRESS || to === WALLET_ADDRESS) {
            // Stores the event in Transfers table.
            const value = decoded.wad;
            await this.set('transfers', `${event.tx_hash}`, {
                from: from,
                to: to,
                value: value,
            });
            // Sets the sign of the balance change depending on whether the wallet is the sender or the receiver.
            const sign = from === WALLET_ADDRESS ? -1 : 1;
            // Updates the balance if it exists, or creates a new balance record.

            const balance = await this.get('balances', WALLET_ADDRESS.toLowerCase());
            if (balance) {
                balance.balance = Number(balance.balance) + sign * value;
                await this.set('balances', WALLET_ADDRESS.toLowerCase(), balance);
            }
            else {
                await this.set('balances', WALLET_ADDRESS.toLowerCase(), { address: WALLET_ADDRESS.toLowerCase(), balance: sign * value });
            }
            // Updates the allowance if it exists and the wallet is the from address.
            // Also checking that spender address is the caller address.
            if (from === WALLET_ADDRESS) {
                const tsunami = new TsunamiApiClient(TSUNAMI_API_KEY, TSUNAMI_API_NET);
                const hash = event.tx_hash;
                const tx = await tsunami.getTransactionWithLogs(hash);
                let msg_sender;
                // Get sender address from the transaction.
                for (const call of tx.logs ?? []) {
                    console.log('Current call: ', call);
                    const right_call = call.op_code === 'CALL' && call.contract === CONTRACT_ADDRESS_LOWERCASE && call.sig_hash === TRANSFER_FROM_SIG_HASH;
                    if (right_call) {
                        msg_sender = call.sender;
                        break;
                    }
                }
                
                if (msg_sender) {
                    const owner_spender = `${from}_${msg_sender}`.toLowerCase();
                    const allowance = await this.get('allowances', owner_spender);
                    if (allowance) {
                        if (allowance.allowance != (2**256 - 1)) {
                            allowance.allowance = Number(allowance.allowance) - value;
                            await this.set('allowances', owner_spender, allowance);
                        }
                    }
                }
            }
        }
    }

    private async processApprovalEvent(event: TsunamiEvent): Promise<void> {
        // Decodes the event...
        const fragment = this.ERC20Decoder.getEvent(event.topic_0!);
        const decoded = this.ERC20Decoder.decodeEventLog(fragment, event.log_data!, [
            event.topic_0!,
            event.topic_1!,
            event.topic_2!
        ]) as unknown as ApprovalEvent;
        // Checks whether the event is relevant to the wallet we are monitoring.
        const owner = decoded.src.toString();
        if (owner === WALLET_ADDRESS) {
            // Updates the allowance if it exists, or creates a new allowance record.
            const spender = decoded.guy.toString();
            const value = decoded.wad;
            // Stores the event in Approvals table.
            await this.set('approvals', `${event.tx_hash}`, {
                owner: owner,
                spender: spender,
                value: value,
            });
            const owner_spender = `${owner}_${spender}`.toLowerCase();
            const allowance = await this.get('allowances', owner_spender);
            if (allowance) {
                allowance.allowance = value;
                await this.set('allowances', owner_spender, allowance);
            }
            else {
                await this.set('allowances', owner_spender, { owner_spender: owner_spender, allowance: value });
            }
        }
    }

    private async processDepositEvent(event: TsunamiEvent): Promise<void> {
        // Decodes the event...
        const fragment = this.ERC20Decoder.getEvent(event.topic_0!);
        const decoded = this.ERC20Decoder.decodeEventLog(fragment, event.log_data!, [
            event.topic_0!,
            event.topic_1!
        ]) as unknown as DepositEvent;
        // Checks whether the event is relevant to the wallet we are monitoring.
        const to = decoded.dst.toString();
        if (to === WALLET_ADDRESS) {
            const value = decoded.wad;
            // Stores the event in Deposits table.
            await this.set('deposits', `${event.tx_hash}`, {
                to: to,
                value: value,
            });
            // Updates the balance if it exists, or creates a new balance record.
            const balance = await this.get('balances', WALLET_ADDRESS.toLowerCase());
            if (balance) {
                balance.balance += value;
                await this.set('balances', WALLET_ADDRESS.toLowerCase(), balance);
            }
            else {
                await this.set('balances', WALLET_ADDRESS.toLowerCase(), { address: WALLET_ADDRESS.toLowerCase(), balance: value });
            }
        }
    }
    

    // The following event handlers should be no-ops under most circumstances.
    public async processEndOfBlockEvent(event: sdk.Block & sdk.DataLakeRunnerState): Promise<void> {}
    public async processBeforeDropBlockEvent(event: sdk.DropBlockData & sdk.DataLakeRunnerState): Promise<void> {}
    public async processAfterDropBlockEvent(event: sdk.DropBlockData & sdk.DataLakeRunnerState): Promise<void> {}
}

export const run = async (): Promise<void> => {
    const logger = new sdk.ConsoleLogger();
    logger.log('DEBUG', 'Initializing datalake...');
    const datalake = new Datalake();
    logger.log('DEBUG', 'Initializing Tsunami API...');
    const tsunami = new TsunamiApiClient(TSUNAMI_API_KEY, TSUNAMI_API_NET);
    logger.log('DEBUG', 'Initializing SDK Tsunami client...');
    const tsunamiSdk = new DatalakeTsunamiApiClient(tsunami);
    logger.log('DEBUG', 'Initializing runner...');
    const runner = new sdk.MultiStorageDataLakeRunner({
        storageConfig: {
            '': { meta: {} },
            'allowances': { meta: {} },
            'balances': { meta: {} },
            'transfers': { meta: {} },
            'transferFroms': { meta: {} },
            'approvals': { meta: {} },
            'deposits': { meta: {} },
        },
        datalake: datalake,
        tsunami: tsunamiSdk,
        log: logger,
    });
    logger.log('DEBUG', 'Running...');
    await runner.run();
}
