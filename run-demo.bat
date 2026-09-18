@echo off
python -m src.producer --count 40 --rate 2 --seed 42 --invalid-rate 0.12 --poison-rate 0.08
