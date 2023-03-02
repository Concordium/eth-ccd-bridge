import type { NextPage } from "next";
import TransferOverview from "@components/templates/transfer-overview";
import { useRef } from "react";
import useCCDWallet from "@hooks/use-ccd-wallet";
import useCCDContract from "src/contracts/use-ccd-contract";
import { routes } from "src/constants/routes";
import { Components } from "src/api-query/__generated__/AxiosClient";
import useEthWallet from "@hooks/use-eth-wallet";
import { useRouter } from "next/router";
import { useNextMerkleRoot } from "src/api-query/queries";
import moment from "moment";

const WithdrawOverview: NextPage = () => {
    const hasApproval = useRef(false);
    const { ccdContext } = useCCDWallet();
    const { context } = useEthWallet();
    const { prefetch } = useRouter();
    const { data, isLoading } = useNextMerkleRoot();
    const nextMerkleTime = data !== undefined ? moment(data * 1000).fromNow() : undefined;

    const {
        withdraw: ccdWithdraw,
        approve: ccdApprove,
        hasApprove,
        estimateApprove,
        transactionFinalization,
        estimateWithdraw,
    } = useCCDContract(ccdContext.account, !!ccdContext.account);

    const requestWithdrawApproval = async (
        token: Components.Schemas.TokenMapItem,
        setStatus: (message: string) => void
    ) => {
        try {
            const approvalFee = await estimateApprove(token);

            setStatus("Awaiting allowance approval in Concordium wallet");
            const hash = await ccdApprove(token, approvalFee);

            setStatus("Waiting for transaction to finalize");
            return await transactionFinalization(hash);
        } catch {
            // Either the allowance approval was rejected, or a timeout happened while polling for allowance approval finalization
            return false;
        }
    };

    /**
     * Handles submission of the withdraw transaction.
     */
    const onSubmit = async (
        token: Components.Schemas.TokenMapItem,
        amount: bigint,
        setError: (message: string) => void,
        setStatus: (message: string) => void
    ): Promise<string | undefined> => {
        if (!context) {
            throw new Error("Could not find Ethereum wallet");
        }

        if (!hasApproval.current) {
            hasApproval.current =
                (await hasApprove({
                    index: token.ccd_contract?.index,
                    subindex: token.ccd_contract?.subindex,
                })) || (await requestWithdrawApproval(token, setStatus));
        }

        if (!hasApproval.current) {
            setError("Approval for withdraw not available");
            return;
        }

        let hash: string | undefined;
        try {
            const withdrawFee = await estimateWithdraw(amount, token, context.account || "");

            setStatus("Awaiting signature of withdrawal in Concordium wallet");
            hash = await ccdWithdraw(amount, token, context?.account || "", withdrawFee);
            prefetch(routes.withdraw.tx(hash));
        } catch {
            setError("Transaction was rejected.");
        }

        if (hash === undefined) {
            return;
        }
        try {
            setStatus("Waiting for transaction to finalize");
            await transactionFinalization(hash); // Wait for transaction finalization, as we do in the deposit flow.

            return routes.withdraw.tx(hash);
        } catch {
            setError("Could not get transaction status for withdrawal");
        }
    };

    return <TransferOverview handleSubmit={onSubmit} isWithdraw nextMerkleRoot={{ isLoading, time: nextMerkleTime }} />;
};

export default WithdrawOverview;
