# Sourced by the scripts in this directory. Every value can be overridden from the environment.

INTEGRATION_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# This Sirius checkout.
export SIRIUS_DIR=${SIRIUS_DIR:-$(git -C "$INTEGRATION_DIR" rev-parse --show-toplevel)}
export SIRIUS_REMOTE=${SIRIUS_REMOTE:-https://github.com/aocsa/sirius.git}
export SIRIUS_BRANCH=${SIRIUS_BRANCH:-$(git -C "$SIRIUS_DIR" rev-parse --abbrev-ref HEAD)}
# The setup/test/bench scripts, next to this checkout by default.
export WORK=${WORK:-$(cd "$SIRIUS_DIR/.." && pwd)}
export TOOLS_REPO=${TOOLS_REPO:-$WORK/starrocks-setup-test-tools}
TOOLS_REMOTE=${TOOLS_REMOTE:-https://github.com/aocsa/starrocks-setup-test-tools.git}
TOOLS_BRANCH=${TOOLS_BRANCH:-box/gb200}
# All 22 TPC-H queries (the tools default to the 7 join queries).
ALL22="q01 q02 q03 q04 q05 q06 q07 q08 q09 q10 q11 q12 q13 q14 q15 q16 q17 q18 q19 q20 q21 q22"
export PATH=$HOME/.pixi/bin:$PATH
