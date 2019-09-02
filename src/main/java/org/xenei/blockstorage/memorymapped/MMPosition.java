package org.xenei.blockstorage.memorymapped;

import org.xenei.spanbuffer.lazy.tree.serde.Position;

public class MMPosition implements Position {
	private final long offset;

	public static final MMPosition NO_DATA = new MMPosition(-1);

	public MMPosition(long offset) {
		this.offset = offset;
	}

	@Override
	public boolean isNoData() {
		return offset == -1;
	}

	public long offset() {
		return offset;
	}

}
