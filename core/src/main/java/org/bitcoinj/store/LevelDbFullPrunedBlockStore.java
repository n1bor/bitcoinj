package org.bitcoinj.store;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.bitcoinj.core.Address;
import org.bitcoinj.core.AddressFormatException;
import org.bitcoinj.core.NetworkParameters;
import org.bitcoinj.core.Sha256Hash;
import org.bitcoinj.core.StoredBlock;
import org.bitcoinj.core.StoredUndoableBlock;
import org.bitcoinj.core.Transaction;
import org.bitcoinj.core.TransactionOutputChanges;
import org.bitcoinj.core.UTXO;
import org.bitcoinj.core.UTXOProviderException;
import org.bitcoinj.core.VerificationException;
import org.bitcoinj.script.Script;
import org.iq80.leveldb.*;

import com.google.common.collect.Lists;

import static org.iq80.leveldb.impl.Iq80DBFactory.*;

import java.io.*;
import java.nio.ByteBuffer;

import javax.annotation.Nullable;

public class LevelDbFullPrunedBlockStore implements FullPrunedBlockStore {
	NetworkParameters params;
	DB db=null;
	
    
    protected Sha256Hash chainHeadHash;
    protected StoredBlock chainHeadBlock;
    protected Sha256Hash verifiedChainHeadHash;
    protected StoredBlock verifiedChainHeadBlock;
    protected int fullStoreDepth;
    protected boolean instrument=false;
    long wallTimeStart;
    protected Map<ByteBuffer,UTXO> utxoCache;
    protected Map<ByteBuffer,UTXO> utxoUncommitedCache;
    protected Set<ByteBuffer> utxoUncommitedDeletedCache;
    protected long hit;
    protected long miss;
    protected String filename;
    
    protected boolean autoCommit=true;
    
	Map<ByteBuffer, byte[]> uncommited;
	Set<ByteBuffer> uncommitedDeletes;
	
	protected long leveldbReadCache;
	protected int leveldbWriteCache;
	protected int openOutCache;
	
	static final long LEVELDB_READ_CACHE_DEFAULT=100 * 1048576; //100 meg
	static final int LEVELDB_WRITE_CACHE_DEFAULT=10 * 1048576; //10 meg
	static final int OPENOUT_CACHE_DEFAULT=100000; 
    
    public class LRUCache extends LinkedHashMap<ByteBuffer,UTXO> {
    	   private static final long serialVersionUID = 1L;
    	   private int capacity;
    	   public LRUCache(int capacity, float loadFactor){
    	      super(capacity, loadFactor, true);
    	      this.capacity = capacity;
    	   }
    	   @Override
    	   protected boolean removeEldestEntry(Map.Entry<ByteBuffer,UTXO> eldest){
    	      return size() > this.capacity;
    	   }
    	}
    
    public LevelDbFullPrunedBlockStore(NetworkParameters params, String filename, int blockCount){
    	this(params,filename,blockCount,LEVELDB_READ_CACHE_DEFAULT,LEVELDB_WRITE_CACHE_DEFAULT,
    			OPENOUT_CACHE_DEFAULT,false);

    }
    


	public LevelDbFullPrunedBlockStore(NetworkParameters params, String filename, int blockCount,
			long leveldbReadCache, int leveldbWriteCache, int openOutCache, boolean instrument){
		this.params=params;
		fullStoreDepth=blockCount;
		this.instrument=instrument;
		methodStartTime=new HashMap<String,Long>();
		methodCalls=new HashMap<String,Long>();
		methodTotalTime=new HashMap<String,Long>();
		wallTimeStart=System.nanoTime();
		utxoCache=new LRUCache(openOutCache,0.75f);
		this.filename=filename;
    	this.leveldbReadCache=leveldbReadCache;
    	this.leveldbWriteCache=leveldbWriteCache;
    	this.openOutCache=openOutCache;
		

		openDB();
	}
	
	private void openDB(){
		Options options = new Options();
		options.createIfMissing(true);
		
		Logger logger = new Logger() {
			  public void log(String message) { 
			    System.out.println(message);
			  }
		};
		options.logger(logger);
		options.cacheSize(leveldbReadCache); 
		options.writeBufferSize(leveldbWriteCache);

		try {
			db = factory.open(new File(filename), options);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		//String stats = batchGetProperty("leveldb.stats");
		//System.out.println(stats);
		try {
			if(batchGet(getKey(KeyType.CREATED))==null) {
					createNewStore(params);
			} else {
					initFromDb();
			}
		} catch (BlockStoreException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			//????
		}
	}
	
	
	
	private void initFromDb() throws BlockStoreException{
        Sha256Hash hash = new Sha256Hash(batchGet(getKey(KeyType.CHAIN_HEAD_SETTING)) );
        this.chainHeadBlock = get(hash);
        this.chainHeadHash = hash;
        if (this.chainHeadBlock == null) {
            throw new BlockStoreException("corrupt database block store - head block not found");
        }

        hash = new Sha256Hash(batchGet(getKey(KeyType.VERIFIED_CHAIN_HEAD_SETTING)));
        this.verifiedChainHeadBlock = get(hash);
        this.verifiedChainHeadHash = hash;
        if (this.verifiedChainHeadBlock == null) {
            throw new BlockStoreException("corrupt databse block store - verified head block not found");
        }		
	}
	
    private void createNewStore(NetworkParameters params) throws BlockStoreException {
        try {
            // Set up the genesis block. When we start out fresh, it is by
            // definition the top of the chain.
            StoredBlock storedGenesisHeader = new StoredBlock(params.getGenesisBlock().cloneAsHeader(), params.getGenesisBlock().getWork(), 0);
            // The coinbase in the genesis block is not spendable. This is because of how the reference client inits
            // its database - the genesis transaction isn't actually in the db so its spent flags can never be updated.
            List<Transaction> genesisTransactions = Lists.newLinkedList();
            StoredUndoableBlock storedGenesis = new StoredUndoableBlock(params.getGenesisBlock().getHash(), genesisTransactions);
            beginDatabaseBatchWrite();
            put(storedGenesisHeader, storedGenesis);
            setChainHead(storedGenesisHeader);
            setVerifiedChainHead(storedGenesisHeader);
            batchPut(getKey(KeyType.CREATED), bytes("done"));
            commitDatabaseBatchWrite();
        } catch (VerificationException e) {
            throw new RuntimeException(e); // Cannot happen.
        }
    }

	Map<String,Long> methodStartTime;
	Map<String,Long> methodCalls;
	Map<String,Long> methodTotalTime;
	void beginMethod(String name){
		methodStartTime.put(name, System.nanoTime());
	}
	void endMethod(String name){
		if(methodCalls.containsKey(name)){
			methodCalls.put(name, methodCalls.get(name)+1);
			methodTotalTime.put(name, methodTotalTime.get(name)+(System.nanoTime()-
					methodStartTime.get(name)));
		} else {
			methodCalls.put(name, 1l);
			methodTotalTime.put(name, System.nanoTime()-
					methodStartTime.get(name));
		}
	}
	
	void dumpStats(){
		long wallTime=System.nanoTime()-wallTimeStart;
		long dbtime=0;
		for(String name:methodCalls.keySet()){
			long calls=methodCalls.get(name);
			long time=methodTotalTime.get(name);
			dbtime+=time;
			long average=time/calls;
			double proportion=(time+0.0)/(wallTime+0.0);
			System.out.println(name+" c:"+calls+" r:"+time+" a:"+average+" p:"+String.format( "%.2f", proportion ));
		}
		double dbproportion=(dbtime+0.0)/(wallTime+0.0);
		double hitrate=(hit+0.0)/(hit+miss+0.0);
		//System.out.println("Cache size:"+utxoCache.size()+" hit:"+hit+" miss:"+miss+" rate:"+String.format( "%.2f", hitrate ));
		System.out.println("Wall:"+wallTime+" percent:"+String.format( "%.2f", dbproportion ));
		
		
	}
	
	@Override
	public void put(StoredBlock block) throws BlockStoreException {
		putUpdateStoredBlock(block, false);

	}



	@Override
	public StoredBlock getChainHead() throws BlockStoreException {
		return chainHeadBlock;
	}

	@Override
	public void setChainHead(StoredBlock chainHead) throws BlockStoreException {
		// TODO Auto-generated method stub
		if(instrument) beginMethod("setChainHead");
        Sha256Hash hash = chainHead.getHeader().getHash();
        this.chainHeadHash = hash;
        this.chainHeadBlock = chainHead;
        batchPut(getKey(KeyType.CHAIN_HEAD_SETTING), hash.getBytes());
		
        if(instrument) endMethod("setChainHead");
	}

	@Override
	public void close() throws BlockStoreException {
		// TODO Auto-generated method stub
		try {
			db.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			throw new BlockStoreException("Could not close db");
		}
	}

	@Override
	public NetworkParameters getParams() {
		// TODO Auto-generated method stub
		return params;
	}

	@Override
	public List<UTXO> getOpenTransactionOutputs(List<Address> addresses)
			throws UTXOProviderException {
		List<UTXO> results=new LinkedList<UTXO>();
		for(Address a:addresses){
			ByteBuffer bb=ByteBuffer.allocate(21);		
			bb.put((byte)KeyType.ADDRESS_HASHINDEX.ordinal());
			bb.put(a.getHash160());


			DBIterator iterator = db.iterator();
			for(iterator.seek(bb.array()); iterator.hasNext(); iterator.next()) {
				ByteBuffer bbKey=ByteBuffer.wrap(iterator.peekNext().getKey());
				bbKey.get(); //remove the address_hashindex byte.
				byte[] addressKey=new byte[20];
				bbKey.get(addressKey);
				if(!Arrays.equals(addressKey, a.getHash160())){
					break;
				}
				byte[] hashBytes=new byte[32];
				bbKey.get(hashBytes);
				int index=bbKey.getInt();
				Sha256Hash hash=new Sha256Hash(hashBytes);
				UTXO txout;
				try {
					txout=getTransactionOutput(hash,index);
					
				} catch (BlockStoreException e) {
					e.printStackTrace();
					throw new UTXOProviderException("block store execption");
				}
				if(txout!=null){
					Script sc=new Script(txout.getScriptBytes());
					Address address=sc.getToAddress(params,true);
                    UTXO output = new UTXO(txout.getHash(),
                    		txout.getIndex(),
                    		txout.getValue(),
                    		txout.getHeight(),
                    		txout.isCoinbase(),
                    		txout.getScriptBytes(),
                            address.toString(),
                            getScriptType(sc).ordinal());
					results.add(output);
				}
					
				
			}
		}
		
		return results;
	}

    private Script.ScriptType getScriptType(@Nullable Script script) {
        if (script != null) {
            return script.getScriptType();
        }
        return Script.ScriptType.NO_TYPE;
    }
	
	
	@Override
	public int getChainHeadHeight() throws UTXOProviderException {
		// TODO Auto-generated method stub
        try {
            return getVerifiedChainHead().getHeight();
        } catch (BlockStoreException e) {
            throw new UTXOProviderException(e);
        }
	}
    protected void putUpdateStoredBlock(StoredBlock storedBlock, boolean wasUndoable) {
    	if(instrument) beginMethod("putUpdateStoredBlock");
    	Sha256Hash hash=storedBlock.getHeader().getHash();
    	ByteBuffer bb=ByteBuffer.allocate(97);
    	storedBlock.serializeCompact(bb);
    	bb.put((byte)(wasUndoable?1:0));
    	batchPut(getKey(KeyType.HEADERS_ALL,hash),bb.array());
    	System.out.println("block "+hash.toString());
    	
    	if(instrument) endMethod("putUpdateStoredBlock");
    }

	@Override
	public void put(StoredBlock storedBlock, StoredUndoableBlock undoableBlock)
			throws BlockStoreException {
		if(instrument) beginMethod("put");
		int height = storedBlock.getHeight();
        byte[] transactions = null;
        byte[] txOutChanges = null;
        try {
            ByteArrayOutputStream bos = new ByteArrayOutputStream();
            if (undoableBlock.getTxOutChanges() != null) {
                undoableBlock.getTxOutChanges().serializeToStream(bos);
                txOutChanges = bos.toByteArray();
            } else {
                int numTxn = undoableBlock.getTransactions().size();
                bos.write((int) (0xFF & (numTxn >> 0)));
                bos.write((int) (0xFF & (numTxn >> 8)));
                bos.write((int) (0xFF & (numTxn >> 16)));
                bos.write((int) (0xFF & (numTxn >> 24)));
                for (Transaction tx : undoableBlock.getTransactions())
                    tx.bitcoinSerialize(bos);
                transactions = bos.toByteArray();
            }
            bos.close();
        } catch (IOException e) {
            throw new BlockStoreException(e);
        }

        Sha256Hash hash=storedBlock.getHeader().getHash();
        
		ByteBuffer keyBuf=ByteBuffer.allocate(33);
		keyBuf.put((byte)KeyType.HEIGHT_UNDOABLEBLOCKS.ordinal());
		keyBuf.putInt(height);
		keyBuf.put(hash.getBytes(),4,28);
		batchPut(keyBuf.array(),new byte[1]);
		
		if (transactions == null) {
        	ByteBuffer undoBuf=ByteBuffer.allocate(4+4+txOutChanges.length+4+0);
        	undoBuf.putInt(height);
        	undoBuf.putInt(txOutChanges.length);
        	undoBuf.put(txOutChanges);
        	undoBuf.putInt(0);
        	batchPut(getKey(KeyType.UNDOABLEBLOCKS_ALL,hash),undoBuf.array());
        } else {
        	ByteBuffer undoBuf=ByteBuffer.allocate(4+4+0+4+transactions.length);
        	undoBuf.putInt(height);
        	undoBuf.putInt(0);
        	undoBuf.putInt(transactions.length);
        	undoBuf.put(transactions);
        	batchPut(getKey(KeyType.UNDOABLEBLOCKS_ALL,hash),undoBuf.array());
        }
        if(instrument) endMethod("put");
        putUpdateStoredBlock(storedBlock, true);
        
	}
	
	enum KeyType {
		CREATED,
		CHAIN_HEAD_SETTING,
		VERIFIED_CHAIN_HEAD_SETTING,
		VERSION_SETTING,
		HEADERS_ALL,
		UNDOABLEBLOCKS_ALL,
		HEIGHT_UNDOABLEBLOCKS,
		OPENOUT_ALL,
		ADDRESS_HASHINDEX
	
	}

	private byte[] getKey(KeyType keytype){
		byte[] key=new byte[1];
		key[0]=(byte)keytype.ordinal();
		return key;
	}
	
	private byte[] getTxKey(KeyType keytype, Sha256Hash hash){
		byte[] key=new byte[33];
		
		key[0]=(byte)keytype.ordinal();
		System.arraycopy(hash.getBytes(), 0, key, 1, 32);
		return key;
	}
	private byte[] getTxKey(KeyType keytype, Sha256Hash hash, int index){
		byte[] key=new byte[37];
		
		key[0]=(byte)keytype.ordinal();
		System.arraycopy(hash.getBytes(), 0, key, 1, 32);
		byte[] heightBytes = ByteBuffer.allocate(4).putInt(index).array();
		System.arraycopy(heightBytes, 0, key, 33, 4);
		return key;
	}
	
	private byte[] getKey(KeyType keytype, Sha256Hash hash){
		byte[] key=new byte[29];
		
		key[0]=(byte)keytype.ordinal();
		System.arraycopy(hash.getBytes(), 4, key, 1, 28);
		return key;
	}
	private byte[] getKey(KeyType keytype, byte[] hash){
		byte[] key=new byte[29];
		
		key[0]=(byte)keytype.ordinal();
		System.arraycopy(hash, 4, key, 1, 28);
		return key;
	}


	@Override
	public StoredBlock getOnceUndoableStoredBlock(Sha256Hash hash)
			throws BlockStoreException {
        return get(hash, true);
	}
	@Override
	public StoredBlock get(Sha256Hash hash) throws BlockStoreException {
        return get(hash, false);
	}

	public StoredBlock get(Sha256Hash hash, boolean wasUndoableOnly) throws BlockStoreException {
			
	        // Optimize for chain head
	        if (chainHeadHash != null && chainHeadHash.equals(hash))
	            return chainHeadBlock;
	        if (verifiedChainHeadHash != null && verifiedChainHeadHash.equals(hash))
	            return verifiedChainHeadBlock;
	        
	        if(instrument) beginMethod("get");
	        boolean undoableResult;

			byte[] result=batchGet(getKey(KeyType.HEADERS_ALL,hash));
			if(result==null){
				if(instrument) endMethod("get");
                return null;
			}
			undoableResult = (result[96]==1?true:false);
            if (wasUndoableOnly && !undoableResult){
            	if(instrument) endMethod("get");
                return null;
            }
            StoredBlock stored= StoredBlock.deserializeCompact(params,ByteBuffer.wrap(result));
            stored.getHeader().verifyHeader();

            if(instrument)endMethod("get");
            return stored;	
			 
	}
	
	
	@Override
	public StoredUndoableBlock getUndoBlock(Sha256Hash hash)
			throws BlockStoreException {
		try {
			if(instrument) beginMethod("getUndoBlock");
			
			byte[] result=batchGet(getKey(KeyType.UNDOABLEBLOCKS_ALL,hash));

			if(result==null){
	        	if(instrument) endMethod("getUndoBlock");
	        	return null;
	        }
	        ByteBuffer bb=ByteBuffer.wrap(result);
	        int height=bb.getInt();
	        int txOutSize=bb.getInt();
	        
	        StoredUndoableBlock block;
	        if (txOutSize == 0) {
	        	int txSize=bb.getInt();
	        	byte[] transactions=new byte[txSize];
	        	bb.get(transactions);
	            int offset = 0;
	            int numTxn = ((transactions[offset++] & 0xFF) << 0) |
	                    ((transactions[offset++] & 0xFF) << 8) |
	                    ((transactions[offset++] & 0xFF) << 16) |
	                    ((transactions[offset++] & 0xFF) << 24);
	            List<Transaction> transactionList = new LinkedList<Transaction>();
	            for (int i = 0; i < numTxn; i++) {
	                Transaction tx = new Transaction(params, transactions, offset);
	                transactionList.add(tx);
	                offset += tx.getMessageSize();
	            }
	            block = new StoredUndoableBlock(hash, transactionList);
	        } else {
	        	byte[] txOutChanges=new byte[txOutSize];
	        	bb.get(txOutChanges);
	            TransactionOutputChanges outChangesObject =
	                    new TransactionOutputChanges(new ByteArrayInputStream(txOutChanges));
	            block = new StoredUndoableBlock(hash, outChangesObject);
	        }
	        if(instrument) endMethod("getUndoBlock");
	        return block;
	    } catch (IOException e) {
	        // Corrupted database.
	    	if(instrument) endMethod("getUndoBlock");
	        throw new BlockStoreException(e);
	    } 
			
	}

	@Override
	public UTXO getTransactionOutput(Sha256Hash hash, long index)
			throws BlockStoreException {
		if(instrument) beginMethod("getTransactionOutput");

		try {
			
			UTXO result=null;
			byte[] key=getTxKey(KeyType.OPENOUT_ALL,hash,(int)index);
			if(autoCommit){
				result=utxoCache.get(ByteBuffer.wrap(key));
			}else{
				if(utxoUncommitedDeletedCache.contains(ByteBuffer.wrap(key))){
					//has been deleted so return null;
					hit++;
					if(instrument) endMethod("getTransactionOutput");
					return result;
				}
				result=utxoUncommitedCache.get(ByteBuffer.wrap(key));
				if(result==null)
					result=utxoCache.get(ByteBuffer.wrap(key));
				
			}
			if(result!=null){
				hit++;
				if(instrument) endMethod("getTransactionOutput");
				return result;
			}
			miss++;
			byte[] inbytes =batchGet(key);
			if (inbytes==null){
				if(instrument) endMethod("getTransactionOutput");
				return null;
			}
			ByteArrayInputStream bis = new ByteArrayInputStream(inbytes);
			UTXO txout=new UTXO(bis);
			 
			
	        if(instrument) endMethod("getTransactionOutput");
	        return txout;
		} catch (DBException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			if(instrument) endMethod("getTransactionOutput");
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			if(instrument) endMethod("getTransactionOutput");
		}
		throw new BlockStoreException("problem");
	}

	@Override
	public void addUnspentTransactionOutput(UTXO out)
			throws BlockStoreException {

		if(instrument) beginMethod("addUnspentTransactionOutput");
		
		
		ByteArrayOutputStream bos = new ByteArrayOutputStream();
		try {
			out.serializeToStream(bos);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			throw new BlockStoreException("problem serialising utxo");
		}
		
	
		byte[] key=getTxKey(KeyType.OPENOUT_ALL,out.getHash(),(int)out.getIndex());
		System.out.println("Tx: "+out.getHash().toString()+":"+out.getIndex());
		batchPut(key,bos.toByteArray());
		if(autoCommit){
			utxoCache.put(ByteBuffer.wrap(key), out);
		} else{
			utxoUncommitedCache.put(ByteBuffer.wrap(key), out);
			utxoUncommitedDeletedCache.remove(ByteBuffer.wrap(key));
		}
		
		//Could run this in parallel with above too.
		Address a;
		if(out.getAddress()==null || out.getAddress().equals("")){
			if(instrument) endMethod("addUnspentTransactionOutput");
			return;
		} else{	
			try {
				a=new Address(params, out.getAddress());
			} catch (AddressFormatException e) {
				// TODO Auto-generated catch block
				if(instrument) endMethod("addUnspentTransactionOutput");
				return;
			}
		}
		ByteBuffer bb=ByteBuffer.allocate(57);		
		bb.put((byte)KeyType.ADDRESS_HASHINDEX.ordinal());
		bb.put(a.getHash160());
		bb.put(out.getHash().getBytes());
		bb.putInt((int)out.getIndex());
		byte[] value=new byte[0];
		batchPut(bb.array(),value);
    	if(instrument) endMethod("addUnspentTransactionOutput");
	}
	
	private void batchPut(byte[] key,byte[] value){
		if(autoCommit){
			db.put(key,value);
		}else{	
			uncommited.put(ByteBuffer.wrap(key), value);
			//System.out.println("uncom: "+uncommited.size());
			batch.put(key,value);
		}	
	}
	private byte[] batchGet(byte[] key){
		ByteBuffer bbKey=ByteBuffer.wrap(key);
		
		// This is needed to cope with deletes that are not yet committed to db.
		// not 100% sure about this - really needs some test case as will never
		// be run on normal block - only one with double spends in it.
		if(!autoCommit && uncommitedDeletes!=null && uncommitedDeletes.contains(bbKey))
			return null;
		
		byte[] value=null;
		
		if(!autoCommit && uncommited !=null ){
			value=uncommited.get(bbKey);
			if(value!=null)
				return value;
		}
		
		value =db.get(key);
		return value;
	}	
	
	private void batchDelete(byte[] key){
		if(!autoCommit){
			batch.delete(key);
			uncommited.remove(ByteBuffer.wrap(key));
			uncommitedDeletes.add(ByteBuffer.wrap(key));
		} else {
			db.delete(key);
		}
		
	}	
	
	@Override
	public void removeUnspentTransactionOutput(UTXO out)
			throws BlockStoreException {
		if(instrument) beginMethod("removeUnspentTransactionOutput");

		byte[] key=getTxKey(KeyType.OPENOUT_ALL,out.getHash(),(int)out.getIndex());
		
		if(autoCommit){
			utxoCache.remove(ByteBuffer.wrap(key));
		} else {
			utxoUncommitedDeletedCache.remove(ByteBuffer.wrap(key));
			utxoUncommitedCache.remove(ByteBuffer.wrap(key));
		}
		
		batchDelete(key);
		//could run this and the above in parallel
		ByteBuffer bb=ByteBuffer.allocate(57);		
		Address a;
		try {
			String address=out.getAddress();
			if(address==null || address.equals("")){
				if(instrument) endMethod("removeUnspentTransactionOutput");
				return;
			}
			a=new Address(params, out.getAddress());
		} catch (AddressFormatException e) {
			e.printStackTrace();
			if(instrument) endMethod("removeUnspentTransactionOutput");
			return;
		}
		bb.put((byte)KeyType.ADDRESS_HASHINDEX.ordinal());
		bb.put(a.getHash160());
		bb.put(out.getHash().getBytes());
		bb.putInt((int)out.getIndex());
		batchDelete(bb.array());
		
		
		if(instrument) endMethod("removeUnspentTransactionOutput");
	}

	@Override
	public boolean hasUnspentOutputs(Sha256Hash hash, int numOutputs)
			throws BlockStoreException {
		if(instrument) beginMethod("hasUnspentOutputs");
		// no index is fine as will find any entry with any index...
		byte[] key=getTxKey(KeyType.OPENOUT_ALL,hash);
		byte[] subResult=new byte[key.length];
		DBIterator iterator = db.iterator();
		for(iterator.seek(key); iterator.hasNext(); ) {
			byte[] result=iterator.peekNext().getKey();
			System.arraycopy(result, 0, subResult, 0, subResult.length);
			if(Arrays.equals(key, subResult)){
				if(instrument) endMethod("hasUnspentOutputs");
				return true;
			} else {
				if(instrument) endMethod("hasUnspentOutputs");
				return false;
			}
		}
		return false;
		
	}

	@Override
	public StoredBlock getVerifiedChainHead() throws BlockStoreException {
		return verifiedChainHeadBlock;
	}

	@Override
	public void setVerifiedChainHead(StoredBlock chainHead)
			throws BlockStoreException {
		if(instrument) beginMethod("setVerifiedChainHead");
	    Sha256Hash hash = chainHead.getHeader().getHash();
        this.verifiedChainHeadHash = hash;
        this.verifiedChainHeadBlock = chainHead;
        batchPut(getKey(KeyType.VERIFIED_CHAIN_HEAD_SETTING), hash.getBytes());
        if (this.chainHeadBlock.getHeight() < chainHead.getHeight())
            setChainHead(chainHead);
        removeUndoableBlocksWhereHeightIsLessThan(chainHead.getHeight() - fullStoreDepth);
        if(instrument) endMethod("setVerifiedChainHead");
	}
	
	void removeUndoableBlocksWhereHeightIsLessThan(int height){
		if (height<0) return;
		DBIterator iterator = db.iterator();
		ByteBuffer keyBuf=ByteBuffer.allocate(5);
		keyBuf.put((byte)KeyType.HEIGHT_UNDOABLEBLOCKS.ordinal());
		keyBuf.putInt(height);

		for(iterator.seek(keyBuf.array()); iterator.hasNext(); iterator.next()) {
			
			byte[] bytekey=iterator.peekNext().getKey();
			ByteBuffer buff=ByteBuffer.wrap(bytekey);
			byte temp=buff.get();
            int keyHeight=buff.getInt();
            
            byte[] hashbytes=new byte[32];
            buff.get(hashbytes,4,28);
            
            if(keyHeight>height)
				break;
            
            batchDelete(getKey(KeyType.UNDOABLEBLOCKS_ALL,hashbytes));

            batchDelete(bytekey);
            
		}            

	}

	WriteBatch batch;
	

	@Override
	public void beginDatabaseBatchWrite() throws BlockStoreException {
		System.out.println("startComm");
		if (!autoCommit){
			/**if(uncommited!=null)
				System.out.println("uncommiteds "+uncommited.size()+" del: "+uncommitedDeletes.size());
			else
				System.out.println("uncommiteds");
				**/
			//db.write(batch);
			return;
		}
		if(instrument) beginMethod("beginDatabaseBatchWrite");
		
		batch = db.createWriteBatch();
		uncommited= new HashMap<ByteBuffer, byte[]>();
		uncommitedDeletes= new HashSet <ByteBuffer>();
	    utxoUncommitedCache=new HashMap<ByteBuffer, UTXO>();
	    utxoUncommitedDeletedCache = new HashSet<ByteBuffer>();
		autoCommit=false;
		if(instrument) endMethod("beginDatabaseBatchWrite");
	}

	
	@Override
	public void commitDatabaseBatchWrite() throws BlockStoreException {
		
		//System.out.println("Commiting uncommiteds "+uncommited.size()+" del: "+uncommitedDeletes.size());
		uncommited=null;
		uncommitedDeletes=null;
		//System.out.println("commit");
		if(instrument) beginMethod("commitDatabaseBatchWrite");
		
		db.write(batch);
		// order of these is not important as we only allow entry to be in one or the other.
		
		for (Map.Entry<ByteBuffer, UTXO> entry : utxoUncommitedCache.entrySet()){
		 
			utxoCache.put(entry.getKey(), entry.getValue());
		}
		utxoUncommitedCache=null;
		for (ByteBuffer entry: utxoUncommitedDeletedCache){
			utxoCache.remove(entry);
		}
		utxoUncommitedDeletedCache=null;
		
		autoCommit=true;
		
		try {
			batch.close();
			batch=null;
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			throw new BlockStoreException("could not close batch.");
		}
		
		if(instrument) endMethod("commitDatabaseBatchWrite");
		
		if (verifiedChainHeadBlock.getHeight()%1000==0){
			System.out.println(verifiedChainHeadBlock.getHeight());
			dumpStats();
			if(verifiedChainHeadBlock.getHeight()==338000){
				System.exit(1); //TODO REMOVE! JUST TO BENCHMARK
			}
		}

		
	}

	@Override
	public void abortDatabaseBatchWrite() throws BlockStoreException {
		// TODO Auto-generated method stub
		//System.out.println("abort");
		try {
			uncommited=null;
			uncommitedDeletes=null;
			autoCommit=true;
			if(batch!=null){
				batch.close();
				batch=null;
			}
			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			throw new BlockStoreException("could not close batch in abort.");
		}
	}
	

	public void resetStore() {
		try {
			db.close();
			uncommited=null;
			uncommitedDeletes=null;
			autoCommit=true;
			utxoCache=new LRUCache(openOutCache,0.75f);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		File f=new File(filename);
		if (f.isDirectory()) {
		    for (File c : f.listFiles())
		    	c.delete();
		  }
		openDB();
	}
	
	
	
}
