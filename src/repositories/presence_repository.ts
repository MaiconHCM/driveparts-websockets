import { Collection, Db } from 'mongodb';

export type StorePresenceDocument = {
  store_id: string;
  last_seen_at: Date;
  updated_at: Date;
};

export class PresenceRepository {
  private readonly presence: Collection<StorePresenceDocument>;

  constructor(db: Db) {
    this.presence = db.collection<StorePresenceDocument>('store_presence');
  }

  async mark_seen(store_id: string, last_seen_at = new Date()): Promise<StorePresenceDocument> {
    const result = await this.presence.findOneAndUpdate(
      { store_id },
      {
        $set: {
          store_id,
          last_seen_at,
          updated_at: last_seen_at
        }
      },
      {
        upsert: true,
        returnDocument: 'after'
      }
    );

    if (!result) {
      throw new Error('store_presence_not_found');
    }

    return result;
  }

  async list_presence(store_ids: string[]): Promise<StorePresenceDocument[]> {
    const unique_store_ids = Array.from(new Set(store_ids.filter(Boolean)));
    if (unique_store_ids.length === 0) {
      return [];
    }

    return this.presence
      .find({ store_id: { $in: unique_store_ids } })
      .toArray();
  }
}
