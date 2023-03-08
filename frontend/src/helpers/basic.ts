export function noOp(): void {
    return undefined;
}

export function isDefined<T>(v?: T): v is T {
    return v !== undefined;
}

export function ensureValue<T>(value: T | undefined, errorMessage: string): T {
    if (value === undefined) {
        throw new Error(errorMessage);
    }

    return value;
}
