Post-campaign environment inventory, not a sample captured during timing:
uname -srmo
lscpu | rg 'Model name|CPU\(s\)|Core\(s\)|Thread'
rg '^MemTotal:' /proc/meminfo
findmnt -T /home/karllivesey/Documents/ChatGPT/VoteBoat/target/p9/overlap411/measure-linux -no FSTYPE,SOURCE
