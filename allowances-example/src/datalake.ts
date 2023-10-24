import * as sdk from '@parsiq/datalake-sdk';
import { TsunamiApiClient, ChainId } from '@parsiq/tsunami-client';
import { TsunamiFilter, TsunamiEvent, DatalakeTsunamiApiClient } from '@parsiq/tsunami-client-sdk-http';
import { Interface } from '@ethersproject/abi';
import * as util from 'util';

// Import your ABIs here, in a format suitable for decoding using @ethersproject/abi.
import votingAbi from './ERC20.json';

// Put your Tsunami API key here.
const TSUNAMI_API_KEY = 'YOUR_API_KEY';
// This is the chain ID for Ethereum mainnet Tsunami API. Change it if you want to work with a different net.
const TSUNAMI_API_NET = ChainId.ETH_MAINNET;

// The address of the wallet that you want to monitor for transfers and approvals.
const WALLET_ADDRESS = '0x7db90684e72a9db431bd4a2a13fcc3412de4de96';
// USDT contract address for starters.
const CONTRACT_ADDRESS = '0xdAC17F958D2ee523a2206206994597C13D831ec7';

// topic_0 hashes of our events of interest.
const TRANSFER_EVENT_TOPIC_0 = '0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef';
const APPROVAL_EVENT_TOPIC_0 = '0x8c5be1e5ebec7d5bd14f71427d1e84f3dd0314c0f7b2291e5b200ac8c7c3b925';

// The block from which you want to monitor the wallet.
const MONITORING_START_BLOCK_NUMBER = 18_419_531;

// This defines the layout of the type-safe K-V storage.
type DatalakeStorageLayout = {
    '': any,
    balance: { _address: string, _balance: number },
    allowance: { _owner_spender: string, _allowance: number },
};

type DatalakeStorageMetaLayout = {
    '': {},
    balance: {},
    allowance: {},
};

// Types for decoded events follow.
type TransferEvent = {
    from: string,
    to: string,
    value: number,
}

type ApprovalEvent = {
    owner: string,
    spender: string,
    value: number,
}

class Datalake extends sdk.AbstractMultiStorageDataLakeBase<DatalakeStorageLayout, DatalakeStorageMetaLayout, TsunamiFilter, TsunamiEvent> {
    private votingDecoder: Interface;

    // Construct ABI decoders here.
    constructor() {
        super();
        this.votingDecoder = new Interface(votingAbi);
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
            topic_0: [TRANSFER_EVENT_TOPIC_0, APPROVAL_EVENT_TOPIC_0],
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
        }
    }

    private async processTransferEvent(event: TsunamiEvent): Promise<void> {
        // Decodes the event.
        const fragment = this.votingDecoder.getEvent(event.topic_0!);
        const decoded = this.votingDecoder.decodeEventLog(fragment, event.log_data!, [
            event.topic_0!,
            event.topic_1!,
            event.topic_2!
        ]) as unknown as TransferEvent;
        // Checks whether the event is relevant to the wallet we are monitoring.
        this.log('INFO', util.inspect(decoded));
        if (decoded.from.toString() === WALLET_ADDRESS || decoded.to.toString() === WALLET_ADDRESS) {
            // Sets the sign of the balance change depending on whether the wallet is the sender or the receiver.
            const sign = decoded.from.toString() === WALLET_ADDRESS ? -1 : 1;
            // Updates the balance if it exists, or creates a new balance record.
            const balance = await this.get('balance', decoded.to.toString());
            if (balance) {
                balance._balance += sign * decoded.value;
                await this.set('balance', decoded.to.toString(), balance);
            }
            else {
                await this.set('balance', decoded.to.toString(), { _address: decoded.to.toString(), _balance: decoded.value });
            }
            // Updates the allowance if it exists and the wallet is the from address.
            const allowance = await this.get('allowance', `${decoded.from.toString()}_${decoded.to.toString()}`);
            if (allowance && decoded.from.toString() === WALLET_ADDRESS) {
                if (allowance._allowance != 2**256 - 1) {
                    allowance._allowance -= decoded.value;
                    await this.set('allowance', `${decoded.from.toString()}_${decoded.to.toString()}`, allowance);
                }
            }
        }
    }

    private async processApprovalEvent(event: TsunamiEvent): Promise<void> {
        // Decodes the event...
        const fragment = this.votingDecoder.getEvent(event.topic_0!);
        const decoded = this.votingDecoder.decodeEventLog(fragment, event.log_data!, [
            event.topic_0!,
            event.topic_1!,
            event.topic_2!
        ]) as unknown as ApprovalEvent;
        // Checks whether the event is relevant to the wallet we are monitoring.
        if (decoded.owner.toString() === WALLET_ADDRESS) {
            // Updates the allowance if it exists, or creates a new allowance record.
            const allowance = await this.get('allowance', `${decoded.owner.toString()}_${decoded.spender.toString()}`);
            if (allowance) {
                allowance._allowance = decoded.value;
                await this.set('allowance', `${decoded.owner.toString()}_${decoded.spender.toString()}`, allowance);
            }
            else {
                await this.set('allowance', `${decoded.owner.toString()}_${decoded.spender.toString()}`, { _owner_spender: `${decoded.owner.toString()}_${decoded.spender.toString()}`, _allowance: decoded.value });
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
            'allowance': { meta: {} },
            'balance': { meta: {} },
        },
        datalake: datalake,
        tsunami: tsunamiSdk,
        log: logger,
    });
    logger.log('DEBUG', 'Running...');
    await runner.run();
}
