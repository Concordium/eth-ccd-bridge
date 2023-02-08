/* Autogenerated file. Do not edit manually. */
/* tslint:disable */
/* eslint-disable */
import {
  BaseContract,
  BigNumber,
  BigNumberish,
  BytesLike,
  CallOverrides,
  ContractTransaction,
  Overrides,
  PayableOverrides,
  PopulatedTransaction,
  Signer,
  utils,
} from "ethers";
import { FunctionFragment, Result } from "@ethersproject/abi";
import { Listener, Provider } from "@ethersproject/providers";
import { TypedEventFilter, TypedEvent, TypedListener, OnEvent } from "./common";

export declare namespace IRootChainManager {
  export type WithdrawParamsStruct = {
    ccdIndex: BigNumberish;
    ccdSubIndex: BigNumberish;
    amount: BigNumberish;
    userWallet: string;
    ccdTxHash: BytesLike;
    ccdEventIndex: BigNumberish;
    tokenId: BigNumberish;
  };

  export type WithdrawParamsStructOutput = [
    BigNumber,
    BigNumber,
    BigNumber,
    string,
    string,
    BigNumber,
    BigNumber
  ] & {
    ccdIndex: BigNumber;
    ccdSubIndex: BigNumber;
    amount: BigNumber;
    userWallet: string;
    ccdTxHash: string;
    ccdEventIndex: BigNumber;
    tokenId: BigNumber;
  };
}

export interface IRootChainManagerInterface extends utils.Interface {
  contractName: "IRootChainManager";
  functions: {
    "cleanMapToken(address,uint64,uint64)": FunctionFragment;
    "depositEtherFor(address,bytes32)": FunctionFragment;
    "depositFor(address,bytes32,address,bytes)": FunctionFragment;
    "mapToken(address,uint64,uint64,bytes32)": FunctionFragment;
    "registerVault(bytes32,address)": FunctionFragment;
    "remapToken(address,uint64,uint64,bytes32)": FunctionFragment;
    "withdraw((uint64,uint64,uint256,address,bytes32,uint64,uint64),bytes32[])": FunctionFragment;
  };

  encodeFunctionData(
    functionFragment: "cleanMapToken",
    values: [string, BigNumberish, BigNumberish]
  ): string;
  encodeFunctionData(
    functionFragment: "depositEtherFor",
    values: [string, BytesLike]
  ): string;
  encodeFunctionData(
    functionFragment: "depositFor",
    values: [string, BytesLike, string, BytesLike]
  ): string;
  encodeFunctionData(
    functionFragment: "mapToken",
    values: [string, BigNumberish, BigNumberish, BytesLike]
  ): string;
  encodeFunctionData(
    functionFragment: "registerVault",
    values: [BytesLike, string]
  ): string;
  encodeFunctionData(
    functionFragment: "remapToken",
    values: [string, BigNumberish, BigNumberish, BytesLike]
  ): string;
  encodeFunctionData(
    functionFragment: "withdraw",
    values: [IRootChainManager.WithdrawParamsStruct, BytesLike[]]
  ): string;

  decodeFunctionResult(
    functionFragment: "cleanMapToken",
    data: BytesLike
  ): Result;
  decodeFunctionResult(
    functionFragment: "depositEtherFor",
    data: BytesLike
  ): Result;
  decodeFunctionResult(functionFragment: "depositFor", data: BytesLike): Result;
  decodeFunctionResult(functionFragment: "mapToken", data: BytesLike): Result;
  decodeFunctionResult(
    functionFragment: "registerVault",
    data: BytesLike
  ): Result;
  decodeFunctionResult(functionFragment: "remapToken", data: BytesLike): Result;
  decodeFunctionResult(functionFragment: "withdraw", data: BytesLike): Result;

  events: {};
}

export interface IRootChainManager extends BaseContract {
  contractName: "IRootChainManager";
  connect(signerOrProvider: Signer | Provider | string): this;
  attach(addressOrName: string): this;
  deployed(): Promise<this>;

  interface: IRootChainManagerInterface;

  queryFilter<TEvent extends TypedEvent>(
    event: TypedEventFilter<TEvent>,
    fromBlockOrBlockhash?: string | number | undefined,
    toBlock?: string | number | undefined
  ): Promise<Array<TEvent>>;

  listeners<TEvent extends TypedEvent>(
    eventFilter?: TypedEventFilter<TEvent>
  ): Array<TypedListener<TEvent>>;
  listeners(eventName?: string): Array<Listener>;
  removeAllListeners<TEvent extends TypedEvent>(
    eventFilter: TypedEventFilter<TEvent>
  ): this;
  removeAllListeners(eventName?: string): this;
  off: OnEvent<this>;
  on: OnEvent<this>;
  once: OnEvent<this>;
  removeListener: OnEvent<this>;

  functions: {
    cleanMapToken(
      rootToken: string,
      childTokenIndex: BigNumberish,
      childTokenSubIndex: BigNumberish,
      overrides?: Overrides & { from?: string | Promise<string> }
    ): Promise<ContractTransaction>;

    depositEtherFor(
      user: string,
      ccdAddress: BytesLike,
      overrides?: PayableOverrides & { from?: string | Promise<string> }
    ): Promise<ContractTransaction>;

    depositFor(
      user: string,
      ccdAddress: BytesLike,
      rootToken: string,
      depositData: BytesLike,
      overrides?: PayableOverrides & { from?: string | Promise<string> }
    ): Promise<ContractTransaction>;

    mapToken(
      rootToken: string,
      childTokenIndex: BigNumberish,
      childTokenSubIndex: BigNumberish,
      tokenType: BytesLike,
      overrides?: Overrides & { from?: string | Promise<string> }
    ): Promise<ContractTransaction>;

    registerVault(
      tokenType: BytesLike,
      vaultAddress: string,
      overrides?: Overrides & { from?: string | Promise<string> }
    ): Promise<ContractTransaction>;

    remapToken(
      rootToken: string,
      childTokenIndex: BigNumberish,
      childTokenSubIndex: BigNumberish,
      tokenType: BytesLike,
      overrides?: Overrides & { from?: string | Promise<string> }
    ): Promise<ContractTransaction>;

    withdraw(
      withdrawParams: IRootChainManager.WithdrawParamsStruct,
      proof: BytesLike[],
      overrides?: PayableOverrides & { from?: string | Promise<string> }
    ): Promise<ContractTransaction>;
  };

  cleanMapToken(
    rootToken: string,
    childTokenIndex: BigNumberish,
    childTokenSubIndex: BigNumberish,
    overrides?: Overrides & { from?: string | Promise<string> }
  ): Promise<ContractTransaction>;

  depositEtherFor(
    user: string,
    ccdAddress: BytesLike,
    overrides?: PayableOverrides & { from?: string | Promise<string> }
  ): Promise<ContractTransaction>;

  depositFor(
    user: string,
    ccdAddress: BytesLike,
    rootToken: string,
    depositData: BytesLike,
    overrides?: PayableOverrides & { from?: string | Promise<string> }
  ): Promise<ContractTransaction>;

  mapToken(
    rootToken: string,
    childTokenIndex: BigNumberish,
    childTokenSubIndex: BigNumberish,
    tokenType: BytesLike,
    overrides?: Overrides & { from?: string | Promise<string> }
  ): Promise<ContractTransaction>;

  registerVault(
    tokenType: BytesLike,
    vaultAddress: string,
    overrides?: Overrides & { from?: string | Promise<string> }
  ): Promise<ContractTransaction>;

  remapToken(
    rootToken: string,
    childTokenIndex: BigNumberish,
    childTokenSubIndex: BigNumberish,
    tokenType: BytesLike,
    overrides?: Overrides & { from?: string | Promise<string> }
  ): Promise<ContractTransaction>;

  withdraw(
    withdrawParams: IRootChainManager.WithdrawParamsStruct,
    proof: BytesLike[],
    overrides?: PayableOverrides & { from?: string | Promise<string> }
  ): Promise<ContractTransaction>;

  callStatic: {
    cleanMapToken(
      rootToken: string,
      childTokenIndex: BigNumberish,
      childTokenSubIndex: BigNumberish,
      overrides?: CallOverrides
    ): Promise<void>;

    depositEtherFor(
      user: string,
      ccdAddress: BytesLike,
      overrides?: CallOverrides
    ): Promise<void>;

    depositFor(
      user: string,
      ccdAddress: BytesLike,
      rootToken: string,
      depositData: BytesLike,
      overrides?: CallOverrides
    ): Promise<void>;

    mapToken(
      rootToken: string,
      childTokenIndex: BigNumberish,
      childTokenSubIndex: BigNumberish,
      tokenType: BytesLike,
      overrides?: CallOverrides
    ): Promise<void>;

    registerVault(
      tokenType: BytesLike,
      vaultAddress: string,
      overrides?: CallOverrides
    ): Promise<void>;

    remapToken(
      rootToken: string,
      childTokenIndex: BigNumberish,
      childTokenSubIndex: BigNumberish,
      tokenType: BytesLike,
      overrides?: CallOverrides
    ): Promise<void>;

    withdraw(
      withdrawParams: IRootChainManager.WithdrawParamsStruct,
      proof: BytesLike[],
      overrides?: CallOverrides
    ): Promise<void>;
  };

  filters: {};

  estimateGas: {
    cleanMapToken(
      rootToken: string,
      childTokenIndex: BigNumberish,
      childTokenSubIndex: BigNumberish,
      overrides?: Overrides & { from?: string | Promise<string> }
    ): Promise<BigNumber>;

    depositEtherFor(
      user: string,
      ccdAddress: BytesLike,
      overrides?: PayableOverrides & { from?: string | Promise<string> }
    ): Promise<BigNumber>;

    depositFor(
      user: string,
      ccdAddress: BytesLike,
      rootToken: string,
      depositData: BytesLike,
      overrides?: PayableOverrides & { from?: string | Promise<string> }
    ): Promise<BigNumber>;

    mapToken(
      rootToken: string,
      childTokenIndex: BigNumberish,
      childTokenSubIndex: BigNumberish,
      tokenType: BytesLike,
      overrides?: Overrides & { from?: string | Promise<string> }
    ): Promise<BigNumber>;

    registerVault(
      tokenType: BytesLike,
      vaultAddress: string,
      overrides?: Overrides & { from?: string | Promise<string> }
    ): Promise<BigNumber>;

    remapToken(
      rootToken: string,
      childTokenIndex: BigNumberish,
      childTokenSubIndex: BigNumberish,
      tokenType: BytesLike,
      overrides?: Overrides & { from?: string | Promise<string> }
    ): Promise<BigNumber>;

    withdraw(
      withdrawParams: IRootChainManager.WithdrawParamsStruct,
      proof: BytesLike[],
      overrides?: PayableOverrides & { from?: string | Promise<string> }
    ): Promise<BigNumber>;
  };

  populateTransaction: {
    cleanMapToken(
      rootToken: string,
      childTokenIndex: BigNumberish,
      childTokenSubIndex: BigNumberish,
      overrides?: Overrides & { from?: string | Promise<string> }
    ): Promise<PopulatedTransaction>;

    depositEtherFor(
      user: string,
      ccdAddress: BytesLike,
      overrides?: PayableOverrides & { from?: string | Promise<string> }
    ): Promise<PopulatedTransaction>;

    depositFor(
      user: string,
      ccdAddress: BytesLike,
      rootToken: string,
      depositData: BytesLike,
      overrides?: PayableOverrides & { from?: string | Promise<string> }
    ): Promise<PopulatedTransaction>;

    mapToken(
      rootToken: string,
      childTokenIndex: BigNumberish,
      childTokenSubIndex: BigNumberish,
      tokenType: BytesLike,
      overrides?: Overrides & { from?: string | Promise<string> }
    ): Promise<PopulatedTransaction>;

    registerVault(
      tokenType: BytesLike,
      vaultAddress: string,
      overrides?: Overrides & { from?: string | Promise<string> }
    ): Promise<PopulatedTransaction>;

    remapToken(
      rootToken: string,
      childTokenIndex: BigNumberish,
      childTokenSubIndex: BigNumberish,
      tokenType: BytesLike,
      overrides?: Overrides & { from?: string | Promise<string> }
    ): Promise<PopulatedTransaction>;

    withdraw(
      withdrawParams: IRootChainManager.WithdrawParamsStruct,
      proof: BytesLike[],
      overrides?: PayableOverrides & { from?: string | Promise<string> }
    ): Promise<PopulatedTransaction>;
  };
}