#include "policy/arc.h"

#include "common/logging.h"

#include <gtest/gtest.h>

namespace mtcache {

class ReplacementArcTest : public testing::Test {
 public:
  void SetUp() override {}

  void TearDown() override {}
};

TEST(ReplacementArcTest, BaseLRUList) {
  BaseLRUList l(3);
  ASSERT_EQ(0, l.Size());
  ASSERT_EQ(3, l.Capacity());

  l.Put("1");  // 1
  l.Put("2");  // 2 1
  l.Put("3");  // 3 2 1
  l.Get("1");  // 1 3 2
  l.Put("4");  // 4 1 3 2
  l.Put("5");  // 5 4 1 3 2
  ASSERT_EQ(5, l.Size());

  auto e = l.Evict();  // 3 2
  ASSERT_EQ(3, l.Size());

  auto t = l.GetTail(10);  // 1 4 5
  ASSERT_EQ(3, l.Size());

  ASSERT_EQ(2, e.size());
  ASSERT_EQ(3, t.size());
  ASSERT_EQ(std::vector<std::string>({"2", "3"}), e);
  ASSERT_EQ(std::vector<std::string>({"1", "4", "5"}), t);
  ASSERT_EQ(std::vector<std::string>({"1", "4", "5"}), l.GetTail(10));
  ASSERT_EQ(3, l.Size());
  ASSERT_EQ(std::vector<std::string>({"1"}), l.EvictOne());
  ASSERT_EQ(2, l.Size());
  ASSERT_EQ(std::vector<std::string>({"4"}), l.EvictOne());
  ASSERT_EQ(1, l.Size());
  ASSERT_EQ(std::vector<std::string>({"5"}), l.EvictOne());
  ASSERT_EQ(0, l.Size());
}

TEST(ReplacementArcTest, GhostLRUList) {
  GhostLRUList glist(10);
  ASSERT_EQ(10, glist.Capacity());
  ASSERT_EQ(10, glist.GhostCapacity());
  ASSERT_EQ(0, glist.Size());
  ASSERT_EQ(0, glist.GhostSize());

  // Put
  for (auto i = 1; i <= 20; ++i) {
    glist.Put(std::to_string(i) + "d");
  }
  ASSERT_EQ(20, glist.Size());
  // Ghost Put
  for (auto i = 1; i <= 20; ++i) {
    glist.PutGhost(std::to_string(i) + "g");
  }
  ASSERT_EQ(20, glist.GhostSize());
  ASSERT_EQ(40, glist.TotalSize());

  // Get
  for (auto i = 1; i <= 10; ++i) {
    glist.Get(std::to_string(i) + "d");
  }  // 10~1
  for (auto i = 1; i <= 10; ++i) {
    glist.Get(std::to_string(i) + "g");
  }  // 10~1
  ASSERT_EQ(std::vector<std::string>({"11d", "12d", "13d"}),
            glist.GetDataTail(3));
  ASSERT_EQ(std::vector<std::string>({"11g", "12g", "13g"}),
            glist.GetGhostTail(3));

  // Downgrade
  for (auto i = 1; i <= 10; ++i) {
    glist.Downgrade();
    ASSERT_EQ(20 - i, glist.Size());
    ASSERT_EQ(20 + i, glist.GhostSize());
    ASSERT_EQ(40, glist.TotalSize());
  }  // 10~1
  auto e = glist.Evict();
  ASSERT_EQ(0, e.size());  // no data evict
  ASSERT_EQ(10, glist.Size());
  ASSERT_EQ(10, glist.GhostSize());
  ASSERT_EQ(std::vector<std::string>({"1d", "2d", "3d"}), glist.GetDataTail(3));
  ASSERT_EQ(std::vector<std::string>({"11d", "12d", "13d"}),
            glist.GetGhostTail(3));
  ASSERT_EQ(10, glist.Size());
  ASSERT_EQ(10, glist.GhostSize());

  // EvictOneData
  ASSERT_EQ(std::vector<std::string>({"1d"}), glist.EvictOneData());
  ASSERT_EQ(std::vector<std::string>({"2d"}), glist.EvictOneData());
  ASSERT_EQ(std::vector<std::string>({"3d"}), glist.EvictOneData());
  ASSERT_EQ(std::vector<std::string>({"4d"}), glist.EvictOneData());
  ASSERT_EQ(std::vector<std::string>({"5d"}), glist.EvictOneData());
  ASSERT_EQ(5, glist.Size());
  // EvictOneGhost
  ASSERT_EQ(std::vector<std::string>({"11d"}), glist.EvictOneGhost());
  ASSERT_EQ(std::vector<std::string>({"12d"}), glist.EvictOneGhost());
  ASSERT_EQ(std::vector<std::string>({"13d"}), glist.EvictOneGhost());
  ASSERT_EQ(std::vector<std::string>({"14d"}), glist.EvictOneGhost());
  ASSERT_EQ(std::vector<std::string>({"15d"}), glist.EvictOneGhost());
  ASSERT_EQ(5, glist.GhostSize());
  // Pop
  auto p = glist.Pop("222");
  ASSERT_EQ("", p.item);
  p = glist.Pop("17d");
  ASSERT_EQ("17d", p.item);
  ASSERT_TRUE(p.is_ghost);
  p = glist.Pop("7d");
  ASSERT_EQ("7d", p.item);
  ASSERT_FALSE(p.is_ghost);
}

TEST(ReplacementArcTest, ArcList) {
  ArcList alist(6);
  ASSERT_EQ(0, alist.Size());
  ASSERT_EQ(0, alist.GhostSize());
  ASSERT_EQ(0, alist.TotalSize());
  ASSERT_EQ(6, alist.Capacity());
  ASSERT_EQ(3, alist.FetchCapacity());
  ASSERT_EQ(3, alist.ActiveCapacity());

  // Put/Get
  alist.Put("1");
  ASSERT_TRUE(alist.Get("1"));
  alist.Put("2");
  ASSERT_TRUE(alist.Get("2"));
  alist.Put("3");
  ASSERT_TRUE(alist.Get("3"));
  ASSERT_FALSE(alist.Get("4"));
  ASSERT_FALSE(alist.Get("5"));
  ASSERT_FALSE(alist.Get("6"));
  // Description:
  // fetch ghost list { fetch data list | active data list } active ghost list
  //
  // symbol | 's left side is fetch list, right side is active list.
  //
  // The item inside {} is data list and outside is ghost list.
  //
  // The item close to | means it has been visited recently, which is the head
  // of the list. Conversely, the tail of th list.
  //
  // example: 8 7 { 2 1 | 3 4 } 5 6
  // fetch data list (head->tail): 1 2
  // fetch ghost list (head->tail): 7 8
  // active data list (head->tail): 3 4
  // active ghost list (head->tail): 5 6

  // { 4 5 6 | 3 2 1 }
  ASSERT_EQ(std::vector<std::string>({"1", "2", "3"}),
            alist.GetActiveDataTail(3));
  ASSERT_EQ(std::vector<std::string>({"4", "5", "6"}),
            alist.GetFetchDataTail(3));
  ASSERT_EQ(std::vector<std::string>({}), alist.GetActiveGhostTail(3));
  ASSERT_EQ(std::vector<std::string>({}), alist.GetFetchGhostTail(3));

  // ARC paper's CaseI
  // move to active
  ASSERT_TRUE(alist.Get("4"));
  // NOTICE: Data is moved to T2(active data list), but T1(fetch data list)
  // capacity(p in paper) will not change
  ASSERT_EQ(3, alist.FetchCapacity());
  ASSERT_EQ(3, alist.ActiveCapacity());

  ASSERT_TRUE(alist.Get("5"));
  ASSERT_EQ(3, alist.FetchCapacity());
  ASSERT_EQ(3, alist.ActiveCapacity());

  ASSERT_TRUE(alist.Get("6"));
  ASSERT_EQ(3, alist.FetchCapacity());
  ASSERT_EQ(3, alist.ActiveCapacity());
  // { | 6 5 4 3 2 1 }
  ASSERT_EQ(std::vector<std::string>({"1", "2", "3"}),
            alist.GetActiveDataTail(3));
  ASSERT_EQ(std::vector<std::string>({}), alist.GetFetchDataTail(3));
  ASSERT_EQ(std::vector<std::string>({}), alist.GetActiveGhostTail(3));
  ASSERT_EQ(std::vector<std::string>({}), alist.GetFetchGhostTail(3));

  // active evict
  ASSERT_FALSE(alist.Get("7"));
  ASSERT_FALSE(alist.Get("8"));
  ASSERT_FALSE(alist.Get("9"));
  // { 7 8 9 | 6 5 4 } 3 2 1
  ASSERT_EQ(std::vector<std::string>({"4", "5", "6"}),
            alist.GetActiveDataTail(3));
  ASSERT_EQ(std::vector<std::string>({"7", "8", "9"}),
            alist.GetFetchDataTail(3));
  ASSERT_EQ(std::vector<std::string>({"1", "2", "3"}),
            alist.GetActiveGhostTail(3));
  ASSERT_EQ(std::vector<std::string>({}), alist.GetFetchGhostTail(3));

  // ARC paper's CaseIII
  // active expand
  ASSERT_FALSE(alist.Get("1"));
  ASSERT_EQ(2, alist.FetchCapacity());
  ASSERT_EQ(4, alist.ActiveCapacity());
  ASSERT_FALSE(alist.Get("2"));
  ASSERT_EQ(1, alist.FetchCapacity());
  ASSERT_EQ(5, alist.ActiveCapacity());
  ASSERT_FALSE(alist.Get("3"));
  ASSERT_EQ(0, alist.FetchCapacity());
  ASSERT_EQ(6, alist.ActiveCapacity());
  // 7 8 9 { | 3 2 1 6 5 4 }
  ASSERT_EQ(std::vector<std::string>({"4", "5", "6"}),
            alist.GetActiveDataTail(3));
  ASSERT_EQ(std::vector<std::string>({}), alist.GetFetchDataTail(3));
  ASSERT_EQ(std::vector<std::string>({}), alist.GetActiveGhostTail(3));
  ASSERT_EQ(std::vector<std::string>({"7", "8", "9"}),
            alist.GetFetchGhostTail(3));

  // fill fetch ghost list full
  ASSERT_FALSE(alist.Get("a"));
  ASSERT_FALSE(alist.Get("b"));
  ASSERT_FALSE(alist.Get("c"));
  // 7 8 9 a b { c | 3 2 1 6 5 } 4
  ASSERT_EQ(std::vector<std::string>({"5", "6", "1"}),
            alist.GetActiveDataTail(3));
  ASSERT_EQ(std::vector<std::string>({"c"}), alist.GetFetchDataTail(3));
  ASSERT_EQ(std::vector<std::string>({"4"}), alist.GetActiveGhostTail(3));
  ASSERT_EQ(std::vector<std::string>({"7", "8", "9"}),
            alist.GetFetchGhostTail(3));

  // ARC paper's CaseII
  // fetch expand
  ASSERT_FALSE(alist.Get("7"));
  ASSERT_EQ(1, alist.FetchCapacity());
  ASSERT_EQ(5, alist.ActiveCapacity());
  ASSERT_FALSE(alist.Get("8"));
  ASSERT_EQ(2, alist.FetchCapacity());
  ASSERT_EQ(4, alist.ActiveCapacity());
  ASSERT_FALSE(alist.Get("9"));
  ASSERT_EQ(3, alist.FetchCapacity());
  ASSERT_EQ(3, alist.ActiveCapacity());
  // a b { c | 9 8 7 3 2 } 1 6 5 4
  ASSERT_EQ(std::vector<std::string>({"2", "3", "7", "8", "9"}),
            alist.GetActiveDataTail(6));
  ASSERT_EQ(std::vector<std::string>({"c"}), alist.GetFetchDataTail(6));
  ASSERT_EQ(std::vector<std::string>({"4", "5", "6", "1"}),
            alist.GetActiveGhostTail(6));
  ASSERT_EQ(std::vector<std::string>({"a", "b"}), alist.GetFetchGhostTail(6));

  // ARC paper's CaseIV B & Replace 2
  // fill fetch list & B2(active ghost list) Evict
  ASSERT_FALSE(alist.Get("d"));
  ASSERT_FALSE(alist.Get("e"));
  ASSERT_FALSE(alist.Get("f"));
  ASSERT_EQ(3, alist.FetchCapacity());
  ASSERT_EQ(3, alist.ActiveCapacity());
  // a b { c d e f | 9 8 } 7 3 2 1
  ASSERT_EQ(std::vector<std::string>({"8", "9"}), alist.GetActiveDataTail(6));
  ASSERT_EQ(std::vector<std::string>({"c", "d", "e", "f"}),
            alist.GetFetchDataTail(6));
  ASSERT_EQ(std::vector<std::string>({"1", "2", "3", "7"}),
            alist.GetActiveGhostTail(6));
  ASSERT_EQ(std::vector<std::string>({"a", "b"}), alist.GetFetchGhostTail(6));

  // ARC paper's Case IV A & Replace 1
  // B1(fetch ghost list) Evict
  ASSERT_FALSE(alist.Get("g"));
  ASSERT_FALSE(alist.Get("h"));
  ASSERT_FALSE(alist.Get("i"));
  ASSERT_EQ(3, alist.FetchCapacity());
  ASSERT_EQ(3, alist.ActiveCapacity());
  // d e { f g h i | 9 8 } 7 3 2 1
  ASSERT_EQ(std::vector<std::string>({"8", "9"}), alist.GetActiveDataTail(6));
  ASSERT_EQ(std::vector<std::string>({"f", "g", "h", "i"}),
            alist.GetFetchDataTail(6));
  ASSERT_EQ(std::vector<std::string>({"1", "2", "3", "7"}),
            alist.GetActiveGhostTail(6));
  ASSERT_EQ(std::vector<std::string>({"d", "e"}), alist.GetFetchGhostTail(6));
}

}  // namespace mtcache

int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
