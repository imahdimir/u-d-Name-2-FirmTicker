"""

    """

from pathlib import Path

import pandas as pd
from githubdata import GitHubDataRepo
from mirutil.df import save_as_prq_wo_index as sprq
from mirutil.ns import rm_ns_module
from mirutil.ns import update_ns_module
from mirutil.str import normalize_completley_and_rm_all_whitespaces as ncr
from mirutil.str import normalize_fa_str_completely as norm_fa

update_ns_module()
import ns

gdu = ns.GDU()
c = ns.Col()

def find_rename_other_col_than_firmticker(df) :
    cols = df.columns
    col = cols.difference([c.ftic])
    col = col[0]
    df = df.rename(columns = {
            col : c.name
            })
    return df

def change_old_firmticker_to_new_firmticker(df) :
    gdm2f = GitHubDataRepo(gdu.src_m2f)
    gdm2f.clone_overwrite()
    dfb = gdm2f.read_data()

    dfb = dfb[[c.btic , c.ftic]]

    dfb[c.btic] = dfb[c.btic].apply(norm_fa)
    dfb = dfb.drop_duplicates()

    dfb = dfb.set_index(c.btic)

    df['norm'] = df[c.ftic].apply(norm_fa)

    df['nf'] = df['norm'].map(dfb[c.ftic])

    msk = df['nf'].notna()
    df.loc[msk , c.ftic] = df.loc[msk , 'nf']

    df = df.drop(columns = ['nf' , 'norm'])
    gdm2f.rmdir()

    return df

def main() :
    pass

    ##
    data_srcs = {
            gdu.src_t2f  : None ,
            gdu.src_ct2f : None ,
            gdu.src_cn2f : None ,
            }

    df = pd.DataFrame()

    for src , _ in data_srcs.items() :
        gdi = GitHubDataRepo(src)
        gdi.clone_overwrite()

        df0 = gdi.read_data()

        df0 = find_rename_other_col_than_firmticker(df0)
        df = pd.concat([df , df0])

        gdi.rmdir()

    ##
    df = df.drop_duplicates()

    ##
    df = change_old_firmticker_to_new_firmticker(df)

    ##
    df = df.drop_duplicates()

    ##
    gduf = GitHubDataRepo(gdu.src_uf)
    gduf.clone_overwrite()

    ##
    dfa = gduf.read_data()

    ##
    msk = df[c.ftic].isin(dfa[c.ftic])

    df1 = df[~msk]

    assert df1.empty , "Not All FirmTickers are in the Unique Firm Tickers List"

    ##
    ermsg = "Each Name key must be at most mapped to one FirmTicker, or None"
    assert df[c.name].is_unique , ermsg

    ##
    dfa = df.copy()
    dfa['nt'] = dfa[c.name].apply(ncr)

    dfa = dfa.drop_duplicates(subset = ['nt' , c.ftic])

    ##
    msk = dfa['nt'].duplicated(keep = False)

    df1 = dfa[msk]

    assert dfa['nt'].is_unique , ermsg

    ##
    gdt = GitHubDataRepo(gdu.trg)
    gdt.clone_overwrite()

    ##
    dffp = gdt.local_path / 'data.prq'
    sprq(df , dffp)

    ##
    msg = 'Updated by: '
    msg += gdu.slf
    print(msg)

    ##
    gdt.commit_and_push(msg)

    ##
    gduf.rmdir()
    gdt.rmdir()

    rm_ns_module()

##


if __name__ == "__main__" :
    main()
    print(f'{Path(__file__).name} Done!')
