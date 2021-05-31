#!/bin/bash
sname='spark-luxmeter-deploy'
tmux new-session -d -s $sname
tmux send-keys -t $sname 'tmux kill-session'
tmux split-window -t $sname -h
tmux send-keys -t $sname '. venv/bin/activate && cd core/src && python -m spark_logs loader' C-m
tmux split-window -t $sname -v
tmux send-keys -t $sname '. venv/bin/activate && cd core/src && python -m spark_logs anomaly-detection' C-m
tmux split-window -t $sname -v
tmux send-keys -t $sname '. venv/bin/activate && cd core/src && python -m spark_logs hybrid-metrics' C-m
tmux split-window -t $sname -v
tmux send-keys -t $sname 'PYTHONPATH="PYTHONPATH:core/src:frontend/src" . venv/bin/activate && cd frontend/src && python -m frontend' C-m
tmux attach-session -t $sname