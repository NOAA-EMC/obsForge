#!/bin/bash -l
# IMPORTANT: -l is needed for cron

set -euo pipefail
# do not create __pucache__ dir:
export PYTHONDONTWRITEBYTECODE=1

OBSFORGE_HOME="/lfs/h2/emc/obsproc/noscrub/edward.givelberg/newmonitor/obsForge"
ASCII_MONITOR_HOME="$OBSFORGE_HOME/utils/ascii-monitor"

RUNDIR="/lfs/h2/emc/obsproc/noscrub/edward.givelberg/newmonitor"
LOGFILE="$RUNDIR/ascii_monitor.log"

START_TS=$(date -u +"%Y-%m-%dT%H:%M:%SZ")

# cd "$RUNDIR"

{
    echo "============================================================"
    echo "START ASCII MONITOR RUN: $START_TS"
    echo "ASCII_MONITOR_HOME = $ASCII_MONITOR_HOME"
    echo "LOGFILE = $LOGFILE"
    echo "============================================================"

    source $OBSFORGE_HOME/ush/of_setup.sh

    python $ASCII_MONITOR_HOME/main.py
    STATUS=$?

    END_TS=$(date -u +"%Y-%m-%dT%H:%M:%SZ")

    echo "============================================================"
    echo "END ASCII MONITOR RUN:   $END_TS (exit=$STATUS)"
    echo "============================================================"
    echo ""

    exit $STATUS
} >> "$LOGFILE" 2>&1
