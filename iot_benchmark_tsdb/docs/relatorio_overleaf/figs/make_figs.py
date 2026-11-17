import sys, matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
OUT=sys.argv[1]
plt.rcParams.update({'font.size':9,'axes.spines.top':False,'axes.spines.right':False,'figure.dpi':150})
U='#1f6fb4'; D='#d9622b'; G='#7a7a7a'; K='#2f9e5a'

# 1 funnel
stages=['Famílias no catálogo (TS0–TS8)','Famílias ativadas pelo workload','Candidatos físicos (Stage 4)',
        'Coberturas válidas V3 (Stage 8)','HBench V3 (Pareto, top-k)','Coberturas V4/Rev2/Rev3\n(fonte canônica × TS4)',
        'HBench Rev3 (A/B)','Variantes nos estudos de\nsensibilidade (S1–Fase 4)']
vals=[9,5,6,24,4,6,3,2]
fig,ax=plt.subplots(figsize=(6.2,3.2))
y=range(len(stages))[::-1]
cols=[G,G,G,G,U,G,U,D]
ax.barh(list(y),vals,color=cols)
for yi,v in zip(y,vals): ax.text(v+0.3,yi,str(v),va='center')
ax.set_yticks(list(y)); ax.set_yticklabels(stages); ax.set_xlabel('quantidade'); ax.set_xlim(0,27)
fig.tight_layout(); fig.savefig(f'{OUT}/fig_funil.pdf')

# 2 D/U vs density
f=[1,2,4,8]
series={'S2 (memtable)':[1.330,1.236,1.166,1.065],'S3 r1 (SSTable)':[1.282,1.167,1.119,1.095],
        'E1 r2 (SSTable)':[1.505,1.241,1.189,1.129],'E1 r3 (SSTable)':[1.319,1.217,1.159,1.136]}
fig,ax=plt.subplots(figsize=(5,2.8))
for (k,v),m in zip(series.items(),'osD^'): ax.plot(f,v,marker=m,label=k)
ax.axhline(1,color='k',lw=0.8,ls='--'); ax.text(1.05,1.01,'empate (D/U = 1)',fontsize=7)
ax.set_xscale('log',base=2); ax.set_xticks(f); ax.set_xticklabels(['1×','2×','4×','8×'])
ax.set_xlabel('fator de densidade (linhas por dispositivo·dia)'); ax.set_ylabel('mediana p95 D / p95 U'); ax.set_ylim(0.95,1.55)
ax.legend(fontsize=7,frameon=False); fig.tight_layout(); fig.savefig(f'{OUT}/fig_du_densidade.pdf')

# 3 Q12 W21
import numpy as np
x=np.arange(4); w=0.2
s2u=[36.828,73.896,149.230,335.982]; s2d=[64.163,98.335,176.019,372.225]
s3u=[38.530,88.390,170.781,392.331]; s3d=[66.871,112.760,235.253,475.098]
fig,ax=plt.subplots(figsize=(5,2.8))
ax.bar(x-1.5*w,s2u,w,color=U,alpha=.55,label='S2 TS2-U'); ax.bar(x-0.5*w,s2d,w,color=D,alpha=.55,label='S2 TS2-D')
ax.bar(x+0.5*w,s3u,w,color=U,label='S3 TS2-U'); ax.bar(x+1.5*w,s3d,w,color=D,label='S3 TS2-D')
ax.set_xticks(x); ax.set_xticklabels(['1×','2×','4×','8×']); ax.set_ylabel('p95 (ms)'); ax.set_xlabel('fator de densidade')
ax.legend(fontsize=7,frameon=False,ncol=2); fig.tight_layout(); fig.savefig(f'{OUT}/fig_q12w21.pdf')

# 4 holdout median D/U
lab=['H1\n21 d','H2\n83 d','H3\n333 d','N3\n3 disp','N18\n18 disp','H2b\n83 d','H3b\n333 d','N6\n6 disp','P1\n17 MB']
r1=[1.306,1.372,1.354,1.518,1.414,1.457,1.418,1.087,1.129]
r2=[1.296,1.364,1.389,1.489,1.377,1.437,1.350,1.218,1.103]
x=np.arange(len(lab)); w=0.38
fig,ax=plt.subplots(figsize=(6.2,2.8))
ax.bar(x-w/2,r1,w,color=U,label='réplica 1'); ax.bar(x+w/2,r2,w,color=K,label='réplica 2')
ax.axhline(1,color='k',lw=0.8,ls='--'); ax.set_ylim(0.9,1.6)
ax.axvline(4.5,color=G,lw=0.8,ls=':'); ax.text(1.5,1.56,'E3/E4 (+E7)',ha='center',fontsize=7); ax.text(6.5,1.56,'E9 (+E15)',ha='center',fontsize=7)
ax.set_xticks(x); ax.set_xticklabels(lab,fontsize=7); ax.set_ylabel('mediana p95 D / p95 U')
ax.legend(fontsize=7,frameon=False,loc='lower left'); fig.tight_layout(); fig.savefig(f'{OUT}/fig_holdout.pdf')

# 5 rules E17
rules=['T5','Rev5','Rev4','MC','U*']
acc=[77.0,77.5,77.5,77.5,80.0]; sev=[11.5,10.0,10.0,10.0,6.25]; reg=[0.101,0.0986,0.0986,0.0986,0.075]
fig,axs=plt.subplots(1,3,figsize=(6.8,2.6))
for a,v,t,c,thr in [(axs[0],acc,'acerto firme (%)',U,90),(axs[1],sev,'erros graves (%)',D,5),(axs[2],reg,'arrependimento p90',G,0.10)]:
    a.bar(range(5),v,color=[c]*4+[K]); a.set_xticks(range(5)); a.set_xticklabels(rules,fontsize=6.5); a.set_title(t,fontsize=8)
    a.axhline(thr,color='k',ls='--',lw=0.8)
axs[0].set_ylim(60,95)
fig.tight_layout(); fig.savefig(f'{OUT}/fig_regras.pdf')
print('ok')
