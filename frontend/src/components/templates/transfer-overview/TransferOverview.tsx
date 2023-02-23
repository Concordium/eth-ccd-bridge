import Button from "@components/atoms/button/Button";
import PageWrapper from "@components/atoms/page-wrapper/PageWrapper";
import useCCDWallet from "@hooks/use-ccd-wallet";
import { useAsyncMemo } from "@hooks/utils";
import Image from "next/image";
import { useRouter } from "next/router";
import { useState } from "react";
import { Components } from "src/api-query/__generated__/AxiosClient";
import { routes } from "src/constants/routes";
import { noOp } from "src/helpers/basic";
import { getPrice } from "src/helpers/price-usd";
import { usePreSubmitStore } from "src/store/pre-submit";
import ConcordiumIcon from "../../../../public/icons/concordium-icon.svg";
import EthereumIcon from "../../../../public/icons/ethereum-icon.svg";
import Text from "../../atoms/text/text";
import { ButtonsContainer, StyledContainer, StyledProcessWrapper } from "./TransferOverview.style";

type BaseProps = {
    /**
     * Callback function for handling submission for specific flow.
     * Expects route of next page to be returned, or undefined if an error happened.
     */
    handleSubmit(
        token: Components.Schemas.TokenMapItem,
        amount: string,
        setError: (message: string) => void
    ): Promise<string | undefined>;
};
type WithdrawProps = BaseProps & {
    isWithdraw: true;
};
type DepositProps = BaseProps & {
    isWithdraw?: false;
    requestGasFee(): Promise<number>;
};

type Props = WithdrawProps | DepositProps;

export const TransferOverview: React.FC<Props> = (props) => {
    const { isWithdraw, handleSubmit } = props;
    const [pendingSubmission, setPendingSubmission] = useState(false);
    const [error, setError] = useState<string>();
    const { back, replace, push } = useRouter();
    const { amount, token: selectedToken, clear: clearPreSubmitState } = usePreSubmitStore();
    const { ccdContext } = useCCDWallet();

    /**
     * Gas fee, only available for deposits (otherwise defaults to 0 and is ignored for withdrawals).
     */
    const gasFee =
        useAsyncMemo(
            async () => {
                if (props.isWithdraw) {
                    return undefined;
                }

                return props.requestGasFee();
            },
            (e) => setError(e.message),
            [props.isWithdraw]
        ) ?? 0;

    const ethPrice = useAsyncMemo(async () => getPrice("ETH"), noOp, []) ?? 0;

    // Check necessary values are present from transfer page. These will not be available if this is the first page loaded in the browser.
    if (!amount || !selectedToken) {
        replace(isWithdraw ? routes.withdraw.path : routes.deposit.path);
    }

    const submit = async () => {
        if (!selectedToken || !amount || !ccdContext.account) {
            throw new Error("Expected page dependencies to be available");
        }

        setPendingSubmission(true);

        const nextRoute = await handleSubmit(selectedToken, amount, setError);

        setPendingSubmission(false);

        if (nextRoute) {
            push(nextRoute);
            clearPreSubmitState();
        }
    };

    return (
        <PageWrapper>
            <StyledContainer>
                <Text fontFamily="Roboto" fontSize="24" fontWeight="light" fontColor="TitleText" fontLetterSpacing="0">
                    {isWithdraw ? "Withdraw Overview" : "Deposit Overview"}
                </Text>

                <div>
                    <Text
                        fontFamily="Roboto"
                        fontSize="13"
                        fontWeight="bold"
                        fontColor="TitleText"
                        fontLetterSpacing="0"
                    >
                        {isWithdraw
                            ? "Wtidhraw should take up to 10 minutes to complete."
                            : "Deposit should take up to 5 minutes to complete."}
                    </Text>
                    <div style={{ marginTop: 12 }} />
                    <Text
                        fontFamily="Roboto"
                        fontSize="13"
                        fontWeight="light"
                        fontColor="TitleText"
                        fontLetterSpacing="0"
                    >
                        Estimation of required fees:
                    </Text>

                    <div style={{ marginTop: 16 }} />
                    <StyledProcessWrapper>
                        <Image
                            src={isWithdraw ? ConcordiumIcon.src : EthereumIcon.src}
                            alt={`${isWithdraw ? "Ethereum Icon" : "Concordium Icon"}`}
                            height="20"
                            width="20"
                        />
                        <Text
                            fontFamily="Roboto"
                            fontSize="11"
                            fontWeight="light"
                            fontColor="TitleText"
                            fontLetterSpacing="0"
                        >
                            {isWithdraw ? "Withdraw initialized:" : "Deposit"}
                        </Text>
                        <Text
                            fontFamily="Roboto"
                            fontSize="11"
                            fontWeight="bold"
                            fontColor="TitleText"
                            fontLetterSpacing="0"
                        >
                            {`${isWithdraw ? "" : "~"}${
                                isWithdraw ? "It will be visible when signing the transaction." : gasFee
                            } ${isWithdraw ? "" : "ETH"} ${isWithdraw ? "" : "("} ${
                                isWithdraw ? "" : (gasFee * ethPrice).toFixed(4)
                            } ${isWithdraw ? "" : "USD)"}`}
                        </Text>
                    </StyledProcessWrapper>

                    {isWithdraw && (
                        <>
                            <StyledProcessWrapper>
                                <Image src={ConcordiumIcon.src} alt="Concordium Icon" height="20" width="20" />
                                <Text
                                    fontFamily="Roboto"
                                    fontSize="11"
                                    fontWeight="light"
                                    fontColor="TitleText"
                                    fontLetterSpacing="0"
                                >
                                    Approve withdraw:
                                </Text>
                                <Text
                                    fontFamily="Roboto"
                                    fontSize="11"
                                    fontWeight="bold"
                                    fontColor="TitleText"
                                    fontLetterSpacing="0"
                                >
                                    It will be visible when signing the transaction.
                                </Text>
                            </StyledProcessWrapper>
                            <div style={{ marginTop: 12 }} />
                            <StyledProcessWrapper>
                                <Image src={EthereumIcon.src} alt={`ccd icon`} height="20" width="20" />
                                <Text
                                    fontFamily="Roboto"
                                    fontSize="11"
                                    fontWeight="light"
                                    fontColor="TitleText"
                                    fontLetterSpacing="0"
                                >
                                    Withdraw complete
                                </Text>
                                <Text
                                    fontFamily="Roboto"
                                    fontSize="11"
                                    fontWeight="bold"
                                    fontColor="TitleText"
                                    fontLetterSpacing="0"
                                >
                                    {!isWithdraw
                                        ? `(${(gasFee * ethPrice).toFixed(4)} USD)`
                                        : "Gas estimation will be available after completing the CCD transaction."}
                                </Text>
                            </StyledProcessWrapper>
                        </>
                    )}
                    {error && (
                        <Text fontSize="12" fontWeight="light" fontColor="Red">
                            {error}
                        </Text>
                    )}
                </div>
                <ButtonsContainer>
                    <Button variant="secondary" onClick={back}>
                        <div style={{ position: "relative" }}>
                            <Text fontSize="16" fontColor="Black" fontWeight="bold">
                                Cancel
                            </Text>
                        </div>
                    </Button>
                    <Button variant="primary" disabled={pendingSubmission} onClick={submit}>
                        <div style={{ position: "relative" }}>
                            <Text fontSize="16" fontColor="Black" fontWeight="bold">
                                Continue
                            </Text>
                        </div>
                    </Button>
                </ButtonsContainer>
            </StyledContainer>
        </PageWrapper>
    );
};