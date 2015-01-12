package org.bitcoinj.examples;

import java.net.InetAddress;
import org.bitcoinj.core.FullPrunedBlockChain;
import org.bitcoinj.core.PeerGroup;
import org.bitcoinj.params.MainNetParams;
import org.bitcoinj.store.FullPrunedBlockStore;
import org.bitcoinj.store.LevelDbFullPrunedBlockStore;

public class LevelDb {
	public static void main(String[] args) throws Exception {
		FullPrunedBlockStore store=new LevelDbFullPrunedBlockStore(MainNetParams.get(),args[0],
				1000,1000*1024*1024l,100*1024*1024,100000,true,Integer.MAX_VALUE);

		FullPrunedBlockChain vChain = new FullPrunedBlockChain(MainNetParams.get(), store);
		vChain.setRunScripts(false);
		
		PeerGroup vPeerGroup = new PeerGroup(MainNetParams.get(), vChain);
        vPeerGroup.setUseLocalhostPeerWhenPossible(true);
        vPeerGroup.addAddress(InetAddress.getLocalHost());
        
        vPeerGroup.start();
        vPeerGroup.downloadBlockChain();
        
	}

}
