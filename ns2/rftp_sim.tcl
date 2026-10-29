# =============================================================================
#  rftp_sim.tcl  -  NS2 simulation of RFTP (Reliable File Transfer over UDP)
#
#  Models the SAME protocol as the Python socket implementation:
#    stop-and-wait ARQ: seq numbers, ACK per packet, timeout + retransmission,
#    duplicate detection at the receiver (duplicate -> data ignored, ACK re-sent)
#  Network: Client(n0) <== duplex link ==> Server(n1) with an ErrorModel
#  (random packet loss) on BOTH directions (data path and ACK path).
#
#  Usage:  ns rftp_sim.tcl <loss> <packets> <timeout_s> <bw> <delay> <chunk_B> <seed> [nam]
#  e.g.    ns rftp_sim.tcl 0.2 150 0.3 1Mb 10ms 1024 1
# =============================================================================
proc arg {i default} {
    global argv
    if {[llength $argv] > $i} { return [lindex $argv $i] }
    return $default
}
set loss     [arg 0 0.1]
set npkts    [arg 1 100]        ;# number of DATA packets (file size / chunk)
set timeout  [arg 2 0.3]
set bw       [arg 3 1Mb]
set delay    [arg 4 10ms]
set chunk    [arg 5 1024]
set seed     [arg 6 1]
set usenam   [arg 7 ""]
set maxretry 50

set ns [new Simulator]
set tf [open rftp_ns2.tr w]
$ns trace-all $tf
if {$usenam eq "nam"} { set nf [open rftp.nam w]; $ns namtrace-all $nf }

# ---------------------------------------------------------------- topology
set n0 [$ns node]     ;# CLIENT (sender)
set n1 [$ns node]     ;# SERVER (receiver)
$ns duplex-link $n0 $n1 $bw $delay DropTail
$ns duplex-link-op $n0 $n1 orient right

# ------------------------------------------- UDP network: random packet loss
set rng [new RNG]
$rng seed $seed
foreach {a b} [list $n0 $n1  $n1 $n0] {
    set rv [new RandomVariable/Uniform]
    $rv use-rng $rng
    set em [new ErrorModel]
    $em set rate_ $loss
    $em unit pkt
    $em ranvar $rv
    $em drop-target [new Agent/Null]
    $ns lossmodel $em $a $b
}

# ------------------------------------------------ UDP-based message agents
set cli [new Agent/Message]
set srv [new Agent/Message]
$ns attach-agent $n0 $cli
$ns attach-agent $n1 $srv
$ns connect $cli $srv
$cli set packetSize_ $chunk
$srv set packetSize_ 40          ;# ACK is a small packet

# ------------------------------------------------------------ RFTP state
set cur_seq 0          ;# 0 = metadata packet, 1..npkts = data packets
set cur_tok 0          ;# identifies the currently running timer
set attempts 0
set data_sent 0        ;# every transmission of a data/meta packet
set retrans 0
set acks_rx 0
set srv_recv 0         ;# distinct packets stored by server
set srv_dup 0          ;# duplicates detected by server
set srv_acks 0
set done 0
set t_start 0.0
set t_end 0.0
set failed 0

proc send_current {} {
    global ns cli cur_seq cur_tok attempts data_sent timeout chunk
    incr attempts
    incr data_sent
    incr cur_tok
    if {$cur_seq == 0} { $cli set packetSize_ 100 } else { $cli set packetSize_ $chunk }
    $cli send "DATA $cur_seq"
    $ns at [expr [$ns now] + $timeout] "on_timeout $cur_seq $cur_tok"   ;# start timer
}

proc on_timeout {seq tok} {
    global cur_seq cur_tok retrans attempts maxretry failed done
    if {$done || $seq != $cur_seq || $tok != $cur_tok} { return }   ;# ACK already arrived
    if {$attempts > $maxretry} { set failed 1; finish_sim; return }
    incr retrans
    send_current                                                    ;# retransmit
}

proc finish_sim {} {
    global done t_end ns
    set done 1
    set t_end [$ns now]
    $ns at [expr [$ns now] + 0.5] "finish"
}

Agent/Message instproc handle {msg} {
    global cli srv cur_seq attempts acks_rx npkts srv_recv srv_dup srv_acks received
    set kind [lindex $msg 0]
    set seq  [lindex $msg 1]
    if {$self eq $srv} {
        # ---------------- SERVER: receive packet, check seq, send ACK
        if {[info exists received($seq)]} { incr ::srv_dup } else { set received($seq) 1; incr srv_recv }
        incr srv_acks
        $srv send "ACK $seq"                 ;# duplicates are re-ACKed
    } else {
        # ---------------- CLIENT: ACK handling
        if {$kind eq "ACK" && $seq == $cur_seq && !$::done} {
            incr acks_rx
            if {$cur_seq == $npkts} { finish_sim; return }
            incr cur_seq
            set attempts 0
            send_current                      ;# next packet
        }
    }
}

proc finish {} {
    global ns tf loss npkts chunk retrans data_sent acks_rx srv_recv srv_dup srv_acks \
           t_start t_end failed timeout bw delay seed
    $ns flush-trace
    close $tf
    set dur [expr $t_end - $t_start]
    set status [expr {$failed ? "FAILED" : "COMPLETED"}]
    puts "RFTP_STATS loss=$loss packets=$npkts chunk=$chunk timeout=$timeout bw=$bw delay=$delay seed=$seed \
transfer_time=$dur sent_total=$data_sent retransmissions=$retrans acks_received=$acks_rx \
server_unique=$srv_recv server_duplicates=$srv_dup server_acks=$srv_acks status=$status"
    exit 0
}

set t_start 0.1
$ns at 0.1 "send_current"
$ns at 100000 "finish"
$ns run
