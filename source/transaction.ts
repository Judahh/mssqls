import { IPool } from '@flexiblepersistence/dao';
import { Transaction as T } from 'mssql';

export class Transaction {
  protected pool: IPool;
  protected transaction: T;

  constructor(pool: IPool) {
    this.pool = pool;
    this.transaction = new T(this.pool);
  }

  public async begin(options?): Promise<void> {
    await new Promise<void>((resolve, reject) => {
      this.transaction.begin(options, (error) => {
        if (error) {
          reject(error);
        } else {
          resolve();
        }
      });
    });
  }

  public async commit(): Promise<void> {
    await new Promise<void>((resolve, reject) => {
      this.transaction.commit((error) => {
        if (error) {
          reject(error);
        } else {
          resolve();
        }
      });
    });
  }

  public async rollback(): Promise<void> {
    await new Promise<void>((resolve, reject) => {
      this.transaction.rollback((error) => {
        if (error) {
          reject(error);
        } else {
          resolve();
        }
      });
    });
  }
}
