class AutoIdIncrementer:
  id = 0

  @classmethod
  def get_id_and_increment(cls):
    this_id = cls.id
    cls.id += 1
    return this_id

  @classmethod
  def reset(cls):
    cls.id = 0
