function transformTraces(data: any): any {
  let traces = data.traces || [];
  if (typeof traces === 'string') {
    try {
      traces = JSON.parse(traces);
    } catch (e) {
      return null;
    }
  }
  if (traces?.length === 0) {
    return null;
  }
  function compareTraceAddress(a: number[], b: number[]): number {
    const aAddr = a || [];
    const bAddr = b || [];
    const minLen = Math.min(aAddr.length, bAddr.length);
    for (let i = 0; i < minLen; i++) {
      if (aAddr[i] !== bAddr[i]) {
        return aAddr[i] - bAddr[i];
      }
    }
    return aAddr.length - bAddr.length;
  }
  traces.sort((a: any, b: any) => compareTraceAddress(a.trace_address, b.trace_address));
  const blockTimestamp = data.block_timestamp || 0;
  function safeLower(value: any): string {
    return typeof value === 'string' ? value.toLowerCase() : '';
  }
  function getCombinedCallType(traceType: string, callType: string): number {
    const normalizedTraceType = traceType.toLowerCase();
    let typeStr = '';
    if (normalizedTraceType === 'call') {
      typeStr = callType.toUpperCase();
    } else {
      typeStr = traceType.toUpperCase();
    }
    switch (typeStr) {
      case 'CALL': return 0;
      case 'CALLCODE': return 1;
      case 'STATICCALL': return 2;
      case 'DELEGATECALL': return 3;
      case 'CREATE': return 4;
      case 'CREATE2': return 5;
      default: return -1;
    }
  }
  function getFunctionSignature(input: string): string {
    if (!input || input?.length < 10) {
      return '0x00000000';
    }
    return input.slice(0, 10).toLowerCase();
  }
  function traceAddressToString(traceAddress: number[]): string {
    const addressWithPrefix = [0, ...traceAddress];
    return addressWithPrefix.join(',');
  }
  function traceAddressKey(traceAddress: number[] | undefined): string {
    if (!traceAddress || traceAddress.length === 0) {
      return '';
    }
    return traceAddress.join(',');
  }
  function buildParentMap(tracesInTx: any[]): Map<string, any> {
    const parentMap = new Map<string, any>();
    for (const trace of tracesInTx) {
      parentMap.set(traceAddressKey(trace.trace_address), trace);
    }
    return parentMap;
  }
  function getParentTrace(currentTrace: any, parentMap: Map<string, any>): any | null {
    const currentAddress = currentTrace.trace_address;
    if (!currentAddress || currentAddress.length === 0) {
      return null;
    }
    return parentMap.get(traceAddressKey(currentAddress.slice(0, -1))) || null;
  }
  const transformed: any[] = [];
  const tracesByTx = new Map<string, any[]>();
  for (const trace of traces) {
    const txHash = trace.transaction_hash;
    if (!tracesByTx.has(txHash)) {
      tracesByTx.set(txHash, []);
    }
    tracesByTx.get(txHash)!.push(trace);
  }
  for (const tracesInTx of tracesByTx.values()) {
    const parentMap = buildParentMap(tracesInTx);
    for (const trace of tracesInTx) {
      const parentTrace = getParentTrace(trace, parentMap);
      const caller = parentTrace ? parentTrace.to_address : trace.from_address;
      const callee = trace.to_address;
      const functionSignature = getFunctionSignature(trace.input);
      const parentFunctionSignature = parentTrace
        ? getFunctionSignature(parentTrace.input)
        : '0x00000000';
      const callDepth = trace.trace_address?.length;
      const traceAddressStr = traceAddressToString(trace.trace_address || []);
      const combinedCallType = getCombinedCallType(trace.trace_type || '', trace.call_type || '');
      const success = trace.status === 1;
      const traceId = `${data.id || ''}_${traceAddressStr}`;
      transformed.push({
        id: traceId,
        block_timestamp: blockTimestamp,
        block_number: trace.block_number ?? 0,
        txn_hash: trace.transaction_hash ?? '',
        caller: caller ? safeLower(caller) : '',
        callee: callee ? safeLower(callee) : '',
        function_signature: functionSignature || '0x00000000',
        parent_function_signature: parentFunctionSignature || '0x00000000',
        tx_from: trace.tx_from_address ? safeLower(trace.tx_from_address) : '',
        tx_to: trace.tx_to_address ? safeLower(trace.tx_to_address) : '',
        call_depth: callDepth ?? 0,
        trace_address: traceAddressStr || '0',
        success: success,
        call_type: combinedCallType ?? -1,
        _gs_op: 'i',
      });
    }
  }
  if (transformed.length > 0) {
    return transformed;
  }
  return null;
}
