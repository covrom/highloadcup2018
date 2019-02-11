// +build amd64

TEXT    ·stdSearch(SB), 4, $0-40
MOVQ    a+0(FP), AX
MOVQ    a_len+8(FP), DI
MOVL    x+24(FP), DX
XORL    SI, SI
other1:
MOVL    DI, CX
loop1:
CMPL    SI, CX
JCC     return1
LEAL    (SI)(CX*1), DI
SHRL    $1, DI
MOVL    (AX)(DI*4), R8
CMPL    R8, DX
JCC     other1
LEAL    1(DI), SI
JMP     loop1
return1:
MOVL    SI, ret+32(FP)
RET
