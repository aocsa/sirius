# Sourced by the multi-CN e2e scripts.
#
# Every CN logs a "leak counters" line after each fragment, receiver drain and FE cancel: what its
# exchange still holds (waiting receivers, parked senders, received batches), how many sender
# fragments the engine keeps parked, and how many direct-exchange buffers are allocated. Between
# queries every count must be zero; one that stays non-zero is GPU memory a finished or failed
# query left behind, which later queries run out of.

# cn_leaks E2E_DIR NUM_CNS [TIMEOUT_S]
# Waits up to TIMEOUT_S (default 20) for the last "leak counters" line of every
# E2E_DIR/cn<i>.log to read all zero. The wait covers the FE's cancel of a failed query, which
# reaches the CNs after the client has its error. Prints what is still held and returns 1 on
# timeout.
cn_leaks() {
    local e2e=$1 num_cns=$2 deadline=$((SECONDS + ${3:-20})) i line
    local -a held
    while true; do
        held=()
        for ((i = 0; i < num_cns; i++)); do
            line=$(grep 'leak counters' "$e2e/cn$i.log" | tail -n 1 || true)
            if grep -qE '\b(receivers|parked_senders|remote_batches|parked_fragments|direct_buffers)=[1-9]' <<<"$line"; then
                held+=("cn$i:${line#*leak counters}")
            fi
        done
        [[ ${#held[@]} -eq 0 ]] && return 0
        if [[ $SECONDS -ge $deadline ]]; then
            printf '   still held: %s\n' "${held[@]}" >&2
            return 1
        fi
        sleep 1
    done
}
